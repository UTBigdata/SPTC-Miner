'Deduplication of replicated passengers and global support aggregation.'


def dedup_one_sid(a, b):
    """Exactly same original trajectory must yield same exact One-off count."""
    if a[0] != b[0]:
        raise ValueError('Conflicting One-off contributions for one (pattern, CardID): '
                         '{} vs {}'.format(a[0], b[0]))
    return (a[0], a[1] | b[1])

def combine_distinct_sids(a, b):
    return (a[0] + b[0], a[1] | b[1])
