import jieba


def context_jieba(data):
    # 通过jieba分词库进行分词
    seg = jieba.cut_for_search(data)
    li = list()
    for word in seg:
        li.append(word)
    return li


def filter_word(date):
    return date not in ["谷", "帮", "客"]


def append_word(date):
    if date == "传智博": date = "传智播客"
    if date == "博学": date = "博学谷"
    if date == "院校": date = "院校帮"
    return date, 1


def extract_user_world(data):
    user_id = data[0]
    content = data[1]
    words = context_jieba(content)
    return_list = []
    for words in words:
        if filter_word(words):
            return_list.append((user_id + '_' + append_word(words)[0], 1))
    return return_list
