def dict_compare(new_dict, old_dict):
    d1_keys = set(new_dict.keys())
    d2_keys = set(old_dict.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o: (new_dict[o], old_dict[o]) for o in intersect_keys if new_dict[o] != old_dict[o]}
    same = set(o for o in intersect_keys if new_dict[o] == old_dict[o])
    return added, removed, modified, same
