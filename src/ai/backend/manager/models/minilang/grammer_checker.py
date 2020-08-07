_START_BRACKET = ['[', '{', '(']
_END_BRACKET = [']', '}', ')']
_EXCEPTION_ESCAPE_STRING = ["\t", "\n", "\b"]


def _is_escape_stirng(string):
    if string >= "A" and string <= "Z":
        return False
    if string >= "a" and string <= "z":
        return False
    if string >= "0" and string <= "9":
        return False
    if string in _START_BRACKET:
        return False
    if string in _END_BRACKET:
        return False
    if string in [":", ",", "_", " "]:
        return False
    return True


def checkQuotes(query):
    checkQ = False
    quotes_cnt = query.count("\"")
    if quotes_cnt % 2 == 0:
        checkQ = True
    return checkQ


def checkBracket(query):
    meetQuotes = False
    bracket_stack = []
    for i in range(len(query)):
        if meetQuotes:
            if query[i] == "\"":
                meetQuotes = False
            continue
        elif query[i] == "\"":
            meetQuotes = True
        else:
            bracket_index = -1
            if query[i] in _START_BRACKET:
                bracket_index = _START_BRACKET.index(query[i])
            if bracket_index != -1:
                bracket_stack.append(bracket_index)
                continue
            if bracket_stack:
                head = bracket_stack[-1]
                if query[i] == _END_BRACKET[head]:
                    bracket_stack.pop()
            elif query[i] in _END_BRACKET:
                return False
    if not bracket_stack:
        return True
    return False


def checkEscapeString(query):
    meetQuotes = False
    for i in range(len(query)):
        if not meetQuotes:
            if query[i] == "\"":
                meetQuotes = True
            elif query[i] in _EXCEPTION_ESCAPE_STRING:
                query[i].replace(query[i], " ")
            elif _is_escape_stirng(query[i]):
                print(query[i])
                return False
        elif query[i] == "\"":
            meetQuotes = False
    return True
