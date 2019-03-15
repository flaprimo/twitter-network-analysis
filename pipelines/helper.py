def str_to_list(s):
    return [] if s == '[]' else s.strip('[]').replace('\'', '').split(', ')
