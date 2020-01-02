def gather_accepted(accepted_list):
    if len(accepted_list) > 0:
        full_accepted = []
        for value in accepted_list:
            items = value.split(',')
            for item in items:
                full_accepted.append(item)
        return full_accepted
    return []


def registeredID(result):
    if 'error' in result.keys():
        return False
    return True


def valid_ark(ark):
    pattern = re.compile("ark:\d+/[\d,\w,-]+")
    if pattern.match(ark):
        return True
    return False
