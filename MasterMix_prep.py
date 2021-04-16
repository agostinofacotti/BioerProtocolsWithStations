from opentrons import protocol_api
import json
import os
import math
from typing import Optional
from itertools import repeat, chain

from opentrons.protocol_api import ProtocolContext
from threading import Thread
import time


class BlinkingLight(Thread):
    def __init__(self, ctx: ProtocolContext, t: float = 1):
        super(BlinkingLight, self).__init__()
        self._on = False
        self._state = True
        self._ctx = ctx
        self._t = t

    def stop(self):
        self._on = False
        self.join()

    def switch(self, x: Optional[bool] = None):
        self._state = not self._state if x is None else x
        self._ctx._hw_manager.hardware.set_lights(rails=self._state)

    def run(self):
        self._on = True
        state = self._ctx._hw_manager.hardware.get_lights()
        while self._on:
            self.switch()
            time.sleep(self._t)
        self.switch(state)

metadata = {
    'protocolName': 'Preparation Mastermix Bioer system',
    'author': 'Giada',
    'source': 'Custom Protocol Request',
    'apiLevel': '2.3'
}

NUM_SAMPLES = 80   #16/32/48/64 (scelta obbligata)
NUM_SEDUTE = 1
NUM_COLONNA = 0  #ATTENZIONE, COMINCIARE A CONTARE DA 0

TIP_TRACK = False

mm_tube_capacity = 1800
mm_strips_capacity = 180
mm_tube_capacity = min(mm_strips_capacity * 8, mm_tube_capacity)

MM_RATE_ASPIRATE = 30
MM_RATE_DISPENSE = 30

mm_mix = {
    "MM x sample": 20
}
liquid_headroom = 1.6
MM_PER_SAMPLE = 20

def run(ctx: protocol_api.ProtocolContext):
    global MM_TYPE

    ctx.comment("Protocollo Preparazione Mastermix Bioer per {} campioni.".format(NUM_SAMPLES))

    # check source (elution) labware type
    tips20 = [
        ctx.load_labware('opentrons_96_filtertiprack_20ul', slot)
        for slot in ['2', '3', '5', '6', '8', '9', '11']
    ]

    tips300 = [ctx.load_labware('opentrons_96_filtertiprack_200ul', '10')]
    pcr_plate = ctx.load_labware(
        'opentrons_96_aluminumblock_biorad_wellplate_200ul', '1', 'PCR plate')
    mm_strips = ctx.load_labware(
        'opentrons_96_aluminumblock_generic_pcr_strip_200ul', '4',
        'mastermix strips')
    tube_block = ctx.load_labware(
        'opentrons_24_aluminumblock_nest_2ml_snapcap', '7',
        '2ml snap tube aluminum block for mastermix + controls')

    # pipette
    m20 = ctx.load_instrument('p20_multi_gen2', 'right', tip_racks=tips20)
    p300 = ctx.load_instrument('p300_single_gen2', 'left', tip_racks=tips300)

    # setup up sample sources and destinations
    num_cols = math.ceil(NUM_SAMPLES / 8)
    sample_dests = pcr_plate.rows()[0][NUM_COLONNA:num_cols + NUM_COLONNA]
    control_dest1 = pcr_plate.wells()[88]  #controlli in posizione A12 e H12
    control_dest2 = pcr_plate.wells()[95]

    tip_log = {'count': {}}
    folder_path = '/data/C'
    tip_file_path = folder_path + '/tip_log.json'
    if TIP_TRACK and not ctx.is_simulating():
        if os.path.isfile(tip_file_path):
            with open(tip_file_path) as json_file:
                data = json.load(json_file)
                if 'tips20' in data:
                    tip_log['count'][m20] = data['tips20']
                else:
                    tip_log['count'][m20] = 0
                if 'tips300' in data:
                    tip_log['count'][p300] = data['tips300']
                else:
                    tip_log['count'][p300] = 0
        else:
            tip_log['count'] = {m20: 0, p300: 0}
    else:
        tip_log['count'] = {m20: 0, p300: 0}

    tip_log['tips'] = {
        m20: [tip for rack in tips20 for tip in rack.rows()[0]],
        p300: [tip for rack in tips300 for tip in rack.wells()]
    }
    tip_log['max'] = {
        pip: len(tip_log['tips'][pip])
        for pip in [m20, p300]
    }

    def pick_up(pip):
        nonlocal tip_log
        if tip_log['count'][pip] == tip_log['max'][pip]:
            # print('Replace ' + str(pip.max_volume) + 'µl tipracks before resuming.')
            ctx.pause('Replace ' + str(pip.max_volume) + 'µl tipracks before resuming.')
            pip.reset_tipracks()
            tip_log['count'][pip] = 0
        pip.pick_up_tip(tip_log['tips'][pip][tip_log['count'][pip]])
        tip_log['count'][pip] += 1

    """ mastermix component maps """
    # setup tube mastermix
    ctx.comment("Mastermix per sample: {}".format(MM_PER_SAMPLE))
    ctx.comment("Num samples: {}".format(NUM_SAMPLES))
    ctx.comment("Liquid headroom: {}".format(liquid_headroom))
    ctx.comment("Tube capacity: {}".format(mm_tube_capacity))

    num_mm_tubes = math.ceil(((MM_PER_SAMPLE * NUM_SAMPLES) + liquid_headroom) / mm_tube_capacity)
    samples_per_mm_tube = []
    
    for i in range(num_mm_tubes):
        remaining_samples = NUM_SAMPLES - sum(samples_per_mm_tube)
        samples_per_mm_tube.append(min(8 * math.ceil(remaining_samples / (8 * (num_mm_tubes - i))), remaining_samples))
    NUM_MM = NUM_SAMPLES + 4
    mm_per_tube = MM_PER_SAMPLE * NUM_MM * 1.1

    mm_tube = tube_block.wells()[:num_mm_tubes]
    ctx.comment("Mastermix: caricare {} tube con almeno {}ul di mastermix".format(num_mm_tubes, mm_per_tube))

    # setup strips mastermix
    mm_strip = mm_strips.columns()[:num_mm_tubes]

    mm_indices = list(chain.from_iterable(repeat(i, ns) for i, ns in enumerate(samples_per_mm_tube)))

    """START REPEATED SECTION"""
    p300.flow_rate.aspirate = MM_RATE_ASPIRATE
    p300.flow_rate.dispense = MM_RATE_DISPENSE
    m20.flow_rate.aspirate = MM_RATE_ASPIRATE
    m20.flow_rate.dispense = MM_RATE_DISPENSE
    for i in range(NUM_SEDUTE):
        ctx.comment("Seduta {}/{}".format(i + 1, NUM_SEDUTE))
        # transfer mastermix to strips
        pick_up(p300)
        for mt, ms, ns in zip(mm_tube, mm_strip, samples_per_mm_tube):
            for strip_i, strip_w in enumerate(ms):
                p300.transfer((ns // 8 + (1 if strip_i < ns % 8 else 0)) * MM_PER_SAMPLE * 1.1, mt.bottom(0.7), strip_w,
                              new_tip='never')
        p300.transfer(MM_PER_SAMPLE, mm_tube[1], control_dest1.bottom(2), new_tip='never')
        p300.transfer(MM_PER_SAMPLE, mm_tube[1], control_dest2.bottom(2), new_tip='never')
        p300.drop_tip()

        # transfer mastermix to plate
        for m_idx, s in zip(mm_indices[::8], sample_dests):
            pick_up(m20)
            m20.transfer(MM_PER_SAMPLE, mm_strip[m_idx][0].bottom(3.5), s.bottom(3.5), new_tip='never')
            m20.drop_tip()

        if i < NUM_SEDUTE - 1:
            blight = BlinkingLight(ctx=ctx)
            blight.start()
            ctx.home()
            ctx.pause("Togliere la pcr plate e preparare l'occorrente per la prossima seduta.")
            blight.stop()
        else:
            blight = BlinkingLight(ctx=ctx)
            blight.start()
            ctx.home()
            ctx.pause("Togliere la pcr plate.")
            blight.stop()

    """END REPEATED SECTION"""

    # track final used tip
    if TIP_TRACK and not ctx.is_simulating():
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)
        data = {
            'tips20': tip_log['count'][m20],
            'tips300': tip_log['count'][p300]
        }
        with open(tip_file_path, 'w') as outfile:
            json.dump(data, outfile)
