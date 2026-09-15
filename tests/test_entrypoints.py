def test_hyp3_mintpy(script_runner):
    ret = script_runner.run(['python', '-m', 'hyp3_mintpy', '-h'])
    assert ret.success
