import os

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


def cleanup_path(paths, removedir=True):
    """ remove unreable files and directories from the input path collection,
    skipped include two type of elements: unwanted directories if removedir is True
    or unaccessible files/directories
    """
    checked = []
    skipped = []

    for ele in paths:
        ele = os.path.abspath(ele)
        if os.path.exists(ele) and os.access(ele, os.R_OK):
            if os.path.isdir(ele) and removedir:
                skipped.append(ele)
            else:
                checked.append(ele)
        else:
            skipped.append(ele)

    return checked, skipped


def identify_copytype(isrc, idest):
    """ verify and return target destination
    case 1: source: multiple files
            destination is an existing directory
            copytype: FILES2DIR
    case 2: source: a file
            destination can either be:
               a file doesn't exist but writable
               a file exists
            then FILE2FILE
    case 3: source: a directory
            destination: a directory doesn't exist, but writable
            then DIR2DIR

    case 3 used to be the only mode FCP supports.
    """

    if not os.path.isabs(idest):
        idest = os.path.abspath(idest)

    single_src_file = True if len(isrc) == 1 and os.path.isfile(isrc[0]) else False
    single_src_dir = True if len(isrc) == 1 and os.path.isdir(isrc[0]) else False

    dest_exist_dir = False
    dest_exist_file = False
    dest_parent_writable = False

    if os.path.exists(idest):
        if not os.access(idest, os.W_OK):
            raise ValueError("Can't access %s" % idest)
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

    if copytype is None:
        raise ValueError("Can't decide the type of copy operations")

    return copytype
