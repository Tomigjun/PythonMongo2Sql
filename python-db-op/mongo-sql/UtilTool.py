def str2PureNum(strs):
    orgCharStr = ''
    for char in strs:
        if(char.isalpha()):
            orgCharStr = orgCharStr + str(ord(char)-65)
        else:
            orgCharStr = orgCharStr + char
    return orgCharStr
