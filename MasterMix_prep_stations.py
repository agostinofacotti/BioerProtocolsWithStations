from opentrons import protocol_api
from covmatic_stations.station import Station, labware_loader, instrument_loader
from covmatic_stations.utils import uniform_divide
from typing import Optional, Tuple
from itertools import repeat
import logging
import math


class BioerMastermixPrep(Station):
    _protocol_description: "Bioer mastermix preparation protocol"

    def __init__(self,
            aspirate_rate: float = 30,
            tube_bottom_headroom_height: float = 2.5,
            strip_bottom_headroom_height: float = 4.0,
            pcr_bottom_headroom_height: float = 4.5,
            dispense_rate: float = 30,
            drop_loc_l: float = 0,
            drop_loc_r: float = 0,
            drop_threshold: int = 296,
            jupyter: bool = True,
            logger: Optional[logging.getLoggerClass()] = None,
            mastermix_vol: float = 20,
            mastermix_vol_headroom: float = 100,
            mastermix_headroom_part_in_strip: float = 0.7,
            mm_strip_capacity: float = 180,
            metadata: Optional[dict] = None,
            num_samples: int = 96,
            control_well_positions = ['A12', 'H12'],
            samples_per_col: int = 8,
            skip_delay: bool = False,
            source_plate_name: str = 'Clean PCR plate on block ',
            tipracks_slots: Tuple[str, ...] = ('2', '3', '5', '6', '8', '9', '11'),
            tube_block_model: str = "opentrons_24_aluminumblock_nest_1.5ml_screwcap",
            tube_max_volume: float = 1800,
            ** kwargs

        ):
        """ Build a :py:class:`.StationC`.
        :param aspirate_rate: Aspiration rate in uL/s
        :param tube_bottom_headroom_height: Height to keep from the bottom for mastermix tubes
        :param strip_bottom_headroom_height: Height to keep from the bottom for the strips
        :param pcr_bottom_headroom_height: Height to keep from the bottom for the output pcr plate
        :param dispense_rate: Dispensation rate in uL/s
        :param drop_loc_l: offset for dropping to the left side (should be positive) in mm
        :param drop_loc_r: offset for dropping to the right side (should be negative) in mm
        :param drop_threshold: the amount of dropped tips after which the run is paused for emptying the trash
        :param logger: logger object. If not specified, the default logger is used that logs through the ProtocolContext comment method
        :param mastermix_vol: Mastermix volume per sample in uL
        :param mastermix_vol_headroom: Headroom for mastermix preparation volume to add to needed volume
        :param metadata: protocol metadata
        :param num_samples: The number of samples that will be loaded on the station B
        :param control_well_positions: Position of the control wells to be filled with mastermix
        :param samples_per_col: The number of samples in a column of the destination plate
        :param source_plate_name: Name for the source plate
        :param skip_delay: If True, pause instead of delay.
        :param tube_block_model: Tube block model name
        """
        super(BioerMastermixPrep, self).__init__(
            drop_loc_l = drop_loc_l,
            drop_loc_r = drop_loc_r,
            drop_threshold = drop_threshold,
            jupyter = jupyter,
            logger = logger,
            metadata = metadata,
            num_samples = num_samples,
            samples_per_col = samples_per_col,
            skip_delay = skip_delay,
            ** kwargs

        )
        self._aspirate_rate = aspirate_rate
        self._tube_bottom_headroom_height = tube_bottom_headroom_height
        self._strip_bottom_headroom_height = strip_bottom_headroom_height
        self._pcr_bottom_headroom_height = pcr_bottom_headroom_height
        self._dispense_rate = dispense_rate
        self._mastermix_vol = mastermix_vol
        self._mastermix_vol_headroom = mastermix_vol_headroom

        assert 0 <= mastermix_headroom_part_in_strip <= 1, \
            "Mastermix headroom in strip part must be between or equal to 0 and 1"
        self._mastermix_headroom_part_in_strip = mastermix_headroom_part_in_strip

        self._mm_strips_capacity = mm_strip_capacity
        self._control_well_positions = control_well_positions
        self._source_plate_name = source_plate_name
        self._tipracks_slots = tipracks_slots
        self._tube_block_model = tube_block_model
        self._tube_max_volume = tube_max_volume

        self._remaining_samples = self._num_samples
        self._done_cols: int = 0

    @labware_loader(1, "_tips20")
    def load_tips20(self):
        self._tips20 = [
            self._ctx.load_labware('opentrons_96_filtertiprack_20ul', slot)
            for slot in self._tipracks_slots
        ]

    @labware_loader(3, "_tips300")
    def loadtips300(self):
        self._tips300 = [self._ctx.load_labware('opentrons_96_filtertiprack_200ul', '10')]

    @labware_loader(5, "_pcr_plate")
    def load_pcr_plate(self):
        self._pcr_plate = self._ctx.load_labware('opentrons_96_aluminumblock_biorad_wellplate_200ul', '1',
                                                 'PCR plate')

    @labware_loader(6, "_mm_strips")
    def load_mm_strips(self):
        self._mm_strips = self._ctx.load_labware('opentrons_96_aluminumblock_generic_pcr_strip_200ul', '4',
                                                 'mastermix strips')

    @labware_loader(7, "_tube_block")
    def load_tube_block(self):
        self._tube_block = self._ctx.load_labware(self._tube_block_model, '7',
                                                  'screw tube aluminum block for mastermix + controls')

    @instrument_loader(0, "_m20")
    def load_m20(self):
        self._m20 = self._ctx.load_instrument('p20_multi_gen2', 'right', tip_racks=self._tips20)
        self._m20.flow_rate.aspirate = self._aspirate_rate
        self._m20.flow_rate.dispense = self._dispense_rate

    @instrument_loader(0, "_p300")
    def load_p300(self):
        self._p300 = self._ctx.load_instrument('p300_single_gen2', 'left', tip_racks=self._tips300)
        self._p300.flow_rate.aspirate = self._aspirate_rate
        self._p300.flow_rate.dispense = self._dispense_rate

    def _tipracks(self) -> dict:
        return {
            "_tips300": "_p300",
            "_tips20": "_m20"
        }

    @property
    def sample_dests(self):
        return self._pcr_plate.rows()[0][:self.num_cols]

    @property
    def sample_dests_wells(self):
        return self._pcr_plate.wells()[:self.num_cols*8]

    @property
    def remaining_cols(self):
        return self.num_cols - self._done_cols

    @property
    def mm_strip(self):
        # We use only one column
        return self._mm_strips.columns()[0]

    @property
    def headroom_from_strip_to_pcr(self):
        return ((self._mastermix_vol_headroom - 1.0) / 2) + 1.0

    @property
    def headroom_vol_from_strip_to_pcr(self):
        print("Headroom strip->pcr: {}ul".format(
            self._mastermix_vol_headroom * self._mastermix_headroom_part_in_strip))
        return self._mastermix_vol_headroom * self._mastermix_headroom_part_in_strip

    @property
    def headroom_vol_from_strip_to_pcr_single(self):
        return self.headroom_vol_from_strip_to_pcr / 8

    @property
    def headroom_vol_from_tubes_to_strip(self):
        print("Headroom tubes->strip: {}ul".format(self._mastermix_vol_headroom * (1 - self._mastermix_headroom_part_in_strip)))
        return self._mastermix_vol_headroom * (1 - self._mastermix_headroom_part_in_strip)

    @property
    def control_dests_wells(self):
        return [self._pcr_plate.wells_by_name()[i] for i in self._control_well_positions]  # controlli in posizione A12 e H12

    def is_well_in_samples(self, well):
        """
        Function that check if a well is within the samples well.
        :param well: well to check
        :return: True if the well is included in the samples list.
        """
        return well in self.sample_dests_wells

    @property
    def control_wells_not_in_samples(self):
        """
        :return: a list of wells for controls that are not already filled with the 8-channel pipette
        """
        return [c for c in self.control_dests_wells if not self.is_well_in_samples(c)]

    def fill_strip(self, volume):
        if not self._p300.has_tip:
            self.pick_up(self._p300)

        total_vol = volume * 8
        self.logger.info("Filling strips with {}ul each; used volume: {}".format(volume, total_vol))

        for well in self.mm_strip:
            self.aspirate_from_tubes(volume, self._p300)
            self._p300.dispense(volume, well.bottom(self._strip_bottom_headroom_height))

        self.drop(self._p300)


    def fill_controls(self):

        if len(self.control_wells_not_in_samples) > 0:
            self.logger.info("Filling controls in {}".format(self.control_wells_not_in_samples))
            if not self._p300.has_tip:
                self.pick_up(self._p300)

            vol = self._mastermix_vol * len(self.control_wells_not_in_samples)
            self.aspirate_from_tubes(vol, self._p300)

            for w in self.control_dests_wells:
                self._p300.dispense(self._mastermix_vol, w.bottom(self._pcr_bottom_headroom_height))
        else:
            self.logger.info("Not filling controls: they will be filled with 8 channel pipette..")

    def aspirate_from_tubes(self, volume, pip):
        aspirate_list = []
        left_volume = volume
        for source_and_vol in self._source_tubes_and_vol:
            if source_and_vol["available_volume"] >= left_volume:
                # aspirate_list.append(dict(source=source_and_vol["source"], vol=left_volume))
                aspirate_vol = left_volume
            else:
                aspirate_vol = source_and_vol["available_volume"]
            left_volume -= aspirate_vol
            source_and_vol["available_volume"] -= aspirate_vol
            if aspirate_vol != 0:
                aspirate_list.append(dict(source=source_and_vol["source"], vol=aspirate_vol))

            if left_volume == 0:
                break
        else:
            raise Exception("No volume left in source tubes.")

        for a in aspirate_list:
            pip.aspirate(a["vol"], a["source"].bottom(self._tube_bottom_headroom_height))

        print("Sources: {}".format(self._source_tubes_and_vol))

    def transfer_to_pcr_plate_and_mark_done(self, num_columns: int):
        num_columns = int(num_columns)
        self.logger.info("Transferring to pcr place {:d} columns.".format(num_columns))
        self.pick_up(self._m20)
        for s in self.get_next_pcr_plate_dests(num_columns):
            self._m20.transfer(self._mastermix_vol,
                               self.mm_strip[0].bottom(self._strip_bottom_headroom_height),
                               s.bottom(self._pcr_bottom_headroom_height),
                               new_tip='never')
        self.drop(self._m20)

    def get_next_pcr_plate_dests(self, num_columns: int):
        if self._done_cols < self.num_cols:
            to_do = int(min(self.num_cols - self._done_cols, num_columns))
        else:
            raise Exception("No more columns to do")
        samples_to_do = self.sample_dests[self._done_cols:self._done_cols+to_do]
        self._done_cols += to_do
        return samples_to_do

    def body(self):
        self.logger.info("Protocol for preparing Bioer mastermix plate.")
        self.logger.info("=============================================\n")
        self.logger.info("Samples: {}".format(self._num_samples))

        self.logger.info("\nIn this run we use a volume overhead of: {}ul".format(self._mastermix_vol_headroom))

        volume_for_controls = len(self.control_wells_not_in_samples) * self._mastermix_vol
        self.logger.info("{}ul will be dispensed to control positions.".format(volume_for_controls))

        volume_for_samples = self._mastermix_vol * self.num_cols * 8
        volume_to_distribute_to_pcr_plate = volume_for_samples + volume_for_controls
        self.logger.info("{}ul will be dispensed to PCR plate".format(volume_to_distribute_to_pcr_plate))
        volume_to_distribute_to_strip = volume_for_samples + self.headroom_vol_from_strip_to_pcr
        self.logger.info("{}ul will be dispensed to strips".format(volume_to_distribute_to_strip))

        total_volume = volume_to_distribute_to_strip + volume_for_controls + self.headroom_vol_from_tubes_to_strip
        self.logger.info("For this run we need a total of {}ul of mastermix".format(total_volume))

        num_tubes, vol_per_tube = uniform_divide(total_volume, self._tube_max_volume)
        self.logger.info("We need {} tubes with {}ul of mastermix each.".format(num_tubes, vol_per_tube))

        mm_tubes = self._tube_block.wells()[:num_tubes]

        # Filling source class to calculate where to aspirate
        self._source_tubes_and_vol = []
        for source in mm_tubes:
            available_volume = (volume_to_distribute_to_strip + volume_for_controls) / len(mm_tubes)
            assert vol_per_tube > available_volume, \
                "Error in volume calcuations: requested {}ul while total in tubes {}ul".format(available_volume,
                                                                                          vol_per_tube)
            self._source_tubes_and_vol.append(dict(source=source,
                                                   available_volume=available_volume))

        # Calculating number of strip fill
        strip_num_fills = 1 + volume_for_samples // (self._mm_strips_capacity * 8)
        self.logger.info("The strip will be filled {} times".format(strip_num_fills))
        strip_headroom_vol_each_fill_single = self.headroom_vol_from_strip_to_pcr_single / strip_num_fills
        self.logger.info("Each fill we add a headroom of {}ul".format(strip_headroom_vol_each_fill_single))
        strip_headroom_vol_single_first_time = self.headroom_vol_from_strip_to_pcr_single

        # First fill controls
        self.fill_controls()

        # Mail loop filling the plate
        while self.remaining_cols > 0:
            # calcuations for filling strip each time
            self.logger.info("\nRemaining cols: {}".format(self.remaining_cols))
            strip_volume = min(self._mm_strips_capacity,
                               self.remaining_cols * self._mastermix_vol + strip_headroom_vol_single_first_time)

            samples_per_this_strip = (strip_volume - strip_headroom_vol_single_first_time) // self._mastermix_vol
            self.logger.info("using that strip for {} samples".format(samples_per_this_strip))

            strip_fill_volume = samples_per_this_strip * self._mastermix_vol + strip_headroom_vol_single_first_time
            strip_headroom_vol_single_first_time = 0 # resetting the headroom volume, will still be present in strips

            self.logger.info("Filling strip with {}ul".format(strip_fill_volume))
            self.fill_strip(strip_fill_volume)

            self.transfer_to_pcr_plate_and_mark_done(samples_per_this_strip)

        if self._p300.has_tip:
            self.drop(self._p300)

        self.logger.debug("Remaining vols: {}".format(self._source_tubes_and_vol))

    def drop(self, pip):
        pip.return_tip()

arguments = dict(num_samples=88)

# protocol for loading in Opentrons App or opentrons_simulate
# =====================================
logging.getLogger(BioerMastermixPrep.__name__).setLevel(logging.INFO)
metadata = {'apiLevel': '2.7'}
station = BioerMastermixPrep(**arguments)


def run(ctx):
    return station.run(ctx)


# for running directly with python command 'py Mastermix_prep_stations.py"
# ========================================================================
if __name__ == "__main__":
    BioerMastermixPrep(**arguments, metadata={'apiLevel': '2.7'}).simulate()
