


def update_segment_defn(segment, value_ ):
    updated_defintion = segment
    if 'preds' in updated_defintion['container']['pred']:
        updated_defintion['container']['pred'] = updated_defintion['container']['pred']['preds'][0]
        if 'str' in updated_defintion['container']['pred']:
            updated_defintion['container']['pred']['str'] = value_
        elif 'num' in updated_defintion['container']['pred']:
            updated_defintion['container']['pred']['num'] = value_
        if updated_defintion['container']['pred']['func'] == 'not-streq':
            updated_defintion['container']['pred']['func'] = 'streq'
        elif updated_defintion['container']['pred']['func'] == 'not-eq':
            updated_defintion['container']['pred']['func'] = 'eq'
    else:
        if 'str' in updated_defintion['container']['pred']:
            updated_defintion['container']['pred']['str'] = value_
        elif 'num' in updated_defintion['container']['pred']:
            updated_defintion['container']['pred']['num'] = value_

    return updated_defintion

def update_segment_negative_defn(segment, value_list):
    updated_defintion = segment
    pred_str = updated_defintion['container']['pred']
    pred_list = []
    for v in value_list:
        #print(v)
        append_str = pred_str.copy()
        if 'str' in append_str:
            append_str['str'] = v
        elif 'num' in append_str:
            append_str['num'] = v
        if pred_str['func'] == 'streq':
            append_str['func'] = 'not-streq'
        elif pred_str['func'] == 'eq':
            append_str['func'] = 'not-eq'
        pred_list.append(append_str)
    #print(pred_list)
    updated_defintion['container']['pred'] = {
        'func': 'and',
        'preds': pred_list
    }
    return updated_defintion


def fix_regions(segment):
    updated_defintion = segment
    #print(updated_defintion)
    updated_defintion['definition']['container']['pred'] =  {'val': {'func': 'attr', 'name': 'variables/georegion'}, 'description': 'Regions', 'func': 'eq', 'num': '3646184479'}
    return updated_defintion