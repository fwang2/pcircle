copytype = None

class CopyType:
    """ A fake enum, define three type of copy job """
    FILE2FILE = 0
    FILE2DIR = 1
    DIR2DIR = 2

def copytype2str(t):
    if t == CopyType.FILE2FILE:
        return "file to file"
    elif t == CopyType.FILE2DIR:
        return "file(s) to dir"
    elif t == CopyType.DIR2DIR:
        return "dir to dir"
    else:
        return "Unknown type"

def remove_errpath(paths):
    """ remove unreable files and directories from the input path collection
    """
    checked = []
    for ele in paths:
        ele = os.path.abspath(ele)
        if os.path.exists(ele) and os.access(ele, os.R_OK):
            checked.append(ele)
        else:
            warn("Skipp non-readable or non-exist: %s" % ele)

    return checked


def check_path(isrc, idest):
    """ verify and return target destination
    case 1: multiple files and directories in the source, destination is an existing directory
            then FILES2DIR
    case 2: source: a file, destination can either be:
            a file doesn't exist but writable
            a file exists
            then FILE2FILE
    case 3: source: a directory
            destination: a directory doesn't exist, but writable
            then DIR2DIR
    """
    global copytype

    isrc = remove_errpath(isrc)
    idest = os.path.abspath(idest)

    single_src_file = True if len(isrc) == 1 and os.path.isfile(isrc[0]) else False
    single_src_dir = True if len(isrc) == 1 and os.path.isdir(isrc[0]) else False

    dest_exist_dir = False
    dest_exist_file = False
    dest_parent_writable = False

    if os.path.exists(idest):
        if not os.access(idest, os.W_OK):
            fatal("Can't access %s" % idest)
        if os.path.isfile(idest):
            dest_exist_file = True
        else:
            dest_exist_dir = True
    else:
        # idest doesn't exist, check its parent
        idest_parent = os.path.dirname(idest)
        if os.path.exists(idest_parent) and os.access(idest_parent, os.W_OK):
            dest_parent_writable = True
    if single_src_file and (dest_exist_file or dest_parent_writable):
        copytype = CopyType.FILE2FILE
    elif single_src_dir and (dest_exist_dir or dest_parent_writable):
        copytype = CopyType.DIR2DIR
    elif not (single_src_dir or single_src_file) and dest_exist_dir:
        copytype = CopyType.FILE2DIR

    logger.debug("dest_exist_dir %r" % dest_exist_dir, extra=DMSG)
    logger.debug("dest_exist_file %r" % dest_exist_file, extra=DMSG)
    logger.debug("dest_parent_writable %r" % dest_parent_writable, extra=DMSG)

    if copytype is None:
        fatal("Can't decide the type of copy operations")

    return isrc, idest

