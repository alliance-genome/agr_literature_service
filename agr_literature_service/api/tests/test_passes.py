def test_passes():
    import os 
    env_keys = os.environ.keys()

    assert "PSQL_HOST" in env_keys