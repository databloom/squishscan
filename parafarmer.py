#!/usr/bin/python3

import os


tldpath="/dlink"

def recursive_file_counter(root):
    cpt = sum([len(files) for r, d, files in os.walk(root)])
    return cpt


def dirlist(root):
    for item in os.listdir(root):
        if os.path.isdir(os.path.join(root, item)):
            return item

def walker(top,maxdepth):
    dirs, nondirs = [], []
    for entry in os.scandir(top):
        (dirs if entry.is_dir() else nondirs).append(entry.path)

    #yield top,dirs, nondirs
    yield dirs
    if maxdepth > 1:
        for path in dirs:
            for x in walker(path, maxdepth-1):
                yield x

def get_tree_size(path):
    """Return total size of files in path and subdirs. If
    is_dir() or stat() fails, print an error message to stderr
    and assume zero size (for example, file has been deleted).
    """
    total = 0
    for entry in os.scandir(path):
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as error:
            print('Error calling is_dir():', error, file=sys.stderr)
            continue
        if is_dir:
            total += get_tree_size(entry.path)
        else:
            try:
                total += entry.stat(follow_symlinks=False).st_size
            except OSError as error:
                print('Error calling stat():', error, file=sys.stderr)
    return total

def humanbytes(B):
   'Return the given bytes as a human friendly KB, MB, GB, or TB string'
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776

   if B < KB:
      return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
   elif KB <= B < MB:
      return '{0:.2f} KB'.format(B/KB)
   elif MB <= B < GB:
      return '{0:.2f} MB'.format(B/MB)
   elif GB <= B < TB:
      return '{0:.2f} GB'.format(B/GB)
   elif TB <= B:
      return '{0:.2f} TB'.format(B/TB)


def walkerwrapper(tldpath):
    for x in walker(tldpath, 1):
        for i in x:
            #return i, humanbytes(get_tree_size(i)))
            return i, get_tree_size(i)

def sortwalker(tldpath):
    sortcounterwalker=0
    sortcounterx=0
    treeitems = [ ]
    for x in walker(tldpath, 2):
        sortcounterwalker=sortcounterwalker+1
        for i in x:
            sortcounterx=sortcounterx+1
            #return i, humanbytes(get_tree_size(i)))
            #treeitems.extend([i, get_tree_size(i)])
            treeitems.append([i, get_tree_size(i)])

    #print(*treeitems, sep = "\n")
    sorted_treeitems = sorted(treeitems, key=lambda x: x[1])
    return sorted_treeitems


#print("gettreesize",humanbytes(get_tree_size(tldpath)))

#print("files in", tldpath, ":",recursive_file_counter(tldpath))

#print("walker",walker(tldpath,5))


#print(walkerwrapper(tldpath))

print(sortwalker(tldpath))
#print("dirlist",dirlist(tldpath))
