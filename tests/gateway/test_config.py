from ai.backend.gateway.config import load as load_config

def test_load_from_toml_file():
    '''
    The path for loading is automatically determined by config.read_from_file method
    from ai.backend.common package.
    The path is "some_path_from_root/halfstack/backend.ai-dev/manager/manager.toml"
    in case of halfstack testing during development
    '''
    args = load_config()
    for key in args.keys():
        assert key in \
            ('_src', 'etcd', 'db', 'manager', 'heartbeat-timeout', 'docker-registry', 'logging', 'logging.pkg-ns', 'logging.console', 'logging.file', 'logging.logstash', 'debug')
    assert 'manager.toml' in str(args['_src'])