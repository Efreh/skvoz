"""Direct server0 composition for independent current runtime fixtures."""
EGRESS_ID = 0


def users(namespace, passwords):
    """Each peer receives only literal-source join/lane permissions."""
    rows = []
    for peer, password in enumerate(passwords):
        if peer == 0:
            pub = [f'{namespace}.join.*.0', f'{namespace}.lane.*.*.0.*.0.*']
            sub = [f'{namespace}.join.0.*', f'{namespace}.lane.0.*.*.*.*.*']
        else:
            pub = [f'{namespace}.join.0.{peer}', f'{namespace}.lane.0.*.{peer % 8}.*.{peer}.*']
            sub = [f'{namespace}.join.{peer}.*', f'{namespace}.lane.{peer}.*.*.*.*.*']
        rows.append({'user': f'p{peer}', 'password': password,
                     'permissions': {'publish': pub, 'subscribe': sub}})
    return rows


def standalone(config, devices):
    """One direct server with a narrow local authoritative assigned set."""
    assigned = [str(peer) for peer in devices]
    config['core'].update(peer_id='0', membership='allowlist', allowed_peers=assigned, initiate=[])
    config['v'] = 3
    config['access'] = {'revision': 1, 'devices': list(devices), 'revoke': []}
