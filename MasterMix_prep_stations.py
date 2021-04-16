from opentrons import protocol_api
from covmatic_stations.station import Station, labware_loader, instrument_loader
from typing import Optional, Tuple
import logging
import math


class BioerMastermixPrep(Station):
    _protocol_description: "Bioer mastermix preparation protocol"

    def __init__(self,
            aspirate_rate: float = 30,
            tube_bottom_headroom_height: float = 3,
            strip_bottom_headroom_height: float = 3.5,
            pcr_bottom_headroom_height: float = 3.5,
            dispense_rate: float = 30,
            drop_loc_l: float = 0,
            drop_loc_r: float = 0,
            drop_threshold: int = 296,
            jupyter: bool = True,
            logger: Optional[logging.getLoggerClass()] = None,
            mastermix_vol: float = 12,
            mastermix_vol_headroom: float = 1.2,
            mastermix_vol_headroom_aspirate: float = 20 / 18,
            metadata: Optional[dict] = None,
            num_samples: int = 96,
            positive_control_well: str = 'A10',
            sample_blow_height: float = -2,
            sample_bottom_height: float = 2,
            sample_mix_vol: float = 10,
            sample_mix_reps: int = 1,
            sample_vol: float = 8,
            samples_per_col: int = 8,
            samples_per_cycle: int = 96,
            skip_delay: bool = False,
            source_plate_name: str = 'chilled elution plate on block from Station B',
            suck_height: float = 2,
            suck_vol: float = 5,
            tempdeck_bool: bool = True,
            tipracks_slots: Tuple[str, ...] = ('2', '3', '5', '6', '8', '9', '11'),
            transfer_samples: bool = True,
            tube_block_model: str = "opentrons_24_aluminumblock_nest_1.5ml_snapcap",
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
        :param mastermix_vol_headroom: Headroom for mastermix preparation volume as a multiplier
        :param mastermix_vol_headroom_aspirate: Headroom for mastermix aspiration volume as a divisor
        :param metadata: protocol metadata
        :param num_samples: The number of samples that will be loaded on the station B
        :param positive_control_well: Position of the positive control well
        :param sample_blow_height: Height from the top when blowing out in mm (should be negative)
        :param sample_bottom_height: Height to keep from the bottom in mm when dealing with samples
        :param sample_mix_vol: Samples mixing volume in uL
        :param sample_mix_reps: Samples mixing repetitions
        :param sample_vol: Sample volume
        :param samples_per_col: The number of samples in a column of the destination plate
        :param samples_per_cycle: The number of samples processable in one cycle
        :param source_plate_name: Name for the source plate
        :param skip_delay: If True, pause instead of delay.
        :param suck_height: Height from the top when sucking in any remaining droplets on way to trash in mm
        :param suck_vol: Volume for sucking in any remaining droplets on way to trash in uL
        :param transfer_samples: Whether to transfer samples or not
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
        self._mastermix_vol_headroom_aspirate = mastermix_vol_headroom_aspirate
        self._positive_control_well = positive_control_well
        self._sample_blow_height = sample_blow_height
        self._sample_bottom_height = sample_bottom_height
        self._sample_mix_vol = sample_mix_vol
        self._sample_mix_reps = sample_mix_reps
        self._sample_vol = sample_vol
        self._samples_per_cycle = int(math.ceil(samples_per_cycle / 8) * 8)
        self._source_plate_name = source_plate_name
        self._suck_height = suck_height
        self._suck_vol = suck_vol
        self._tempdeck_bool = tempdeck_bool
        self._tipracks_slots = tipracks_slots
        self._transfer_samples = transfer_samples
        self._tube_block_model = tube_block_model

        self._remaining_samples = self._num_samples
        self._samples_this_cycle = min(self._remaining_samples, self._samples_per_cycle)

    @property
    def num_cycles(self) -> int:
        return int(math.ceil(self._num_samples / self._samples_per_cycle))

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

    def body(self):
        self.logger.info("Bioer protocol started!")

if __name__ == "__main__":
    BioerMastermixPrep(num_samples=1, metadata={'apiLevel': '2.3'}).simulate()