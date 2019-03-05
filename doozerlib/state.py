STATE_PEND = 'pending'
STATE_PASS = 'passed'
STATE_FAIL = 'failed'
STATE_PART = 'partial'

TEMPLATE_BASE_STATE = {
    'status': STATE_PASS,
    'msg': 'Complete'
}

TEMPLATE_IMAGE = {
    'status': STATE_PEND,
    'msg': '',
    'images': {},
    'total': 0,
    'success': 0,
    'required_fail': 0,
    'optional_fail': 0
}


def record_image_success(state, image):
    state['success'] += 1
    state['images'][image.name] = True


def record_image_fail(state, image, msg, logger=None):
    state['required_fail' if image.required else 'optional_fail'] += 1
    state['images'][image.name] = msg
    if logger:
        logger.error('[{}] {}'.format(image.qualified_key, msg))


def record_image_finish(state, msg='Complete'):
    if state['required_fail']:
        state['status'] = STATE_FAIL
    elif state['optional_fail']:
        state['status'] = STATE_PART
    elif state['total'] == state['success']:
        state['status'] = STATE_PASS
