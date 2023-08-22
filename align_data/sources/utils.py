def merge_dicts(*dicts):
    final = {}
    for d in dicts:
        final = dict(final, **{k: v for k, v in d.items() if v is not None})
    return final
