"""Current standalone routing composition for independent runtime fixtures."""
import copy

EGRESS_ID = (1 << 63) + 8


def users(namespace, passwords, authority_password):
    """Exact client principals, literal-source egress, and control authority."""
    rows = []
    for peer, password in enumerate(passwords):
        identity = EGRESS_ID if peer == 0 else peer
        if peer == 0:
            pub = [f'{namespace}.join.*.{identity}', f'{namespace}.lane.*.*.0.*.{identity}.*',
                   f'{namespace}.route.node.{identity}']
            sub = [f'{namespace}.join.{identity}.*', f'{namespace}.lane.{identity}.*.*.*.*.*',
                   f'{namespace}.route.command.{identity}', f'{namespace}.route.reply.node.{identity}']
        else:
            pub = [f'{namespace}.join.{EGRESS_ID}.{peer}', f'{namespace}.lane.{EGRESS_ID}.*.{peer % 8}.*.{peer}.*',
                   f'{namespace}.route.client.{peer}']
            sub = [f'{namespace}.join.{peer}.*', f'{namespace}.lane.{peer}.*.*.*.*.*',
                   f'{namespace}.route.reply.client.{peer}']
        rows.append({'user': f'p{peer}', 'password': password,
                     'permissions': {'publish': pub, 'subscribe': sub}})
    rows.append({'user': 'authority', 'password': authority_password, 'permissions': {
        'publish': [f'{namespace}.route.command.*', f'{namespace}.route.reply.client.*', f'{namespace}.route.reply.node.*'],
        'subscribe': [f'{namespace}.route.client.*', f'{namespace}.route.node.*']}})
    return rows


def standalone(config, authority_password, devices):
    """One application hosts the common authority and egress implementations."""
    core = config['core']
    core.update(peer_id=str(EGRESS_ID), membership='allowlist', allowed_peers=[], initiate=[])
    authority = copy.deepcopy(core)
    authority.update(peer_id='0', username='authority', password=authority_password)
    config['v'] = 2
    config['routing'] = {'egress': True, 'authority': {'core': authority,
        'registry': {'revision': 1, 'devices': list(devices), 'nodes': [EGRESS_ID]}}}
