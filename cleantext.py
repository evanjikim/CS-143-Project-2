#!/usr/bin/env python

# """Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json

__author__ = ""
__email__ = ""

# Some useful data.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    # """Do parse the text in variable "text" according to the spec, and return
    # a LIST containing FOUR strings
    # 1. The parsed text.
    # 2. The unigrams
    # 3. The bigrams
    # 4. The trigrams
    # """

    # YOUR CODE GOES BELOW:
    parsed_text = re.sub('\s+', ' ', text).strip()
    parsed_text = re.sub('(http\S+)', '', parsed_text)
    parsed_text = re.sub('(www\.\S+)','',parsed_text)
    special = re.findall("[^a-zA-Z0-9_.,;:!? '\-]", parsed_text)
    within = []

    for i in special:
        reg = "(\w+" + re.escape(i) + "\w+ )"
        mult = "(\w+" + re.escape(i) + "\w+" + re.escape(i) + "\w+)"
        within += re.findall(reg, parsed_text)
        within += re.findall(mult,parsed_text)

    parsed_text = re.sub("[^a-zA-Z0-9_.,;:!? '\-]", '', parsed_text)

    word = re.findall("(\A-\w+)", parsed_text)
    word += re.findall("( -\w+)", parsed_text)
    word += re.findall("(\w+- )", parsed_text)
    word += re.findall("(\w+' )", parsed_text)
    word += re.findall("(\A'\w+)", parsed_text)
    word += re.findall("( '\w+)", parsed_text)

    for i in word:
        new = re.sub("[-']", '', i)
        parsed_text = parsed_text.replace(i,new)

    contraction = re.findall("(\w+'\w+)", parsed_text)

    for i in contraction:
        if i not in _CONTRACTIONS.values():
            new = re.sub("'",'', i)
            parsed_text = parsed_text.replace(i,new)

    parsed_text = re.sub('( tis )', " 'tis ", parsed_text)
    parsed_text = re.sub('(\Atis )', "'tis ", parsed_text)
    parsed_text = re.sub('( ol )', " 'ol ", parsed_text)
    parsed_text = re.sub('(\Aol )', "'ol ", parsed_text)

    parsed_text = ' '.join(re.findall(r"[\w'-]+|[.,!?;:]",parsed_text))
    for i in within:
        for j in special:
             s = "(\w+" + re.escape(j) + "\w+ )"
             # m = "(\w+" + re.escape(j) + "\w+" + re.escape(j) + "\w+)"
             if re.match(s,i):
                 temp = re.sub(re.escape(j), '', i)
                 parsed_text = re.sub(temp, i, parsed_text)


    parsed_text = parsed_text.lower()
    count = len(parsed_text.split())
    temp = list(enumerate(parsed_text.split()))
    unigrams = ''
    for i, word in enumerate(parsed_text.split()):
        if i<count and re.match("[^.,;:!?]",word):
            if not unigrams:
                unigrams += word
            else:
                str = ' ' + word
                unigrams += str


    bigrams = ''

    for i, word in enumerate(parsed_text.split()):
        if i+1<count and re.match("[^.,;:!?]",word) and re.match('[^.,:;!?]',temp[i+1][1]):
            if not bigrams:
                str = word + '_' + (temp[i+1][1])
                bigrams += str
            else:
                str = ' ' + word + '_' +(temp[i+1][1])
                bigrams += str

    trigrams = ''
    for i, word in enumerate(parsed_text.split()):
        if i+2<count and re.match("[^.,;:!?]",word) and re.match('[^.,:;!?]',temp[i+1][1]) and re.match('[^.,:;!?]',temp[i+2][1]):
            if not trigrams:
                str = word + '_' + (temp[i+1][1]) + '_' + (temp[i+2][1])
                trigrams += str
            else:
                str = ' ' + word + '_' +(temp[i+1][1]) + '_' + (temp[i+2][1])
                trigrams += str

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('r'))
    args = parser.parse_args()

    filename = args.file


    for line in filename:
        data = json.loads(line)
        text = data['body'].encode('ascii','ignore')
        print(sanitize(text))
