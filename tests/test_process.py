import subprocess
from pathlib import Path

from hyp3_mintpy.process import rename_products, write_cfg


def test_rename_products(script_runner):
    Path('test/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000').mkdir(parents=True)
    with Path(
        'test/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000.txt'
    ).open('w') as test:
        test.write('S1_000000_IW1_00000000T000000_VV_AAAA-BURST')

    rename_products('test')
    folder = Path('test/S1_000000_IW1_00000000_00000000_VV_INT80_0000')
    txt = folder / 'S1_000000_IW1_00000000_00000000_VV_INT80_0000.txt'

    assert folder.is_dir()
    assert txt.exists()

    subprocess.call('rm -rf test', shell=True)


def test_write_cfg(script_runner):
    job_name = 'test_job'
    min_coherence = '0.5'
    write_cfg(job_name, min_coherence)

    assert Path(f'{job_name}/MintPy/{job_name}.txt').exists()

    with Path(f'{job_name}/MintPy/{job_name}.txt').open() as cfg:
        lines = cfg.readlines()

    minCoh = 0
    for line in lines:
        if 'minCoherence' in line:
            minCoh = float(line.split('=')[1])

    assert minCoh == float(min_coherence)

    subprocess.call(f'rm -rf {job_name}', shell=True)
