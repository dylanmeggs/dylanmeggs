#import modules
import sys
import random
import re

#initial variables
where = ["WHERE ARE WE", "WHERE", "WHERE ARE YOU", "WHERE CAN I"]
why = ["WHY NOT", "WHY", "HOWCOME", "HUH"]
who = ["WHO ARE YOU"]
meaning = ["WHAT IS THE MEANING OF LIFE"]
decisions = ["SHOULD I", "WHAT DO I DO", "HOW CAN I"]
creator = ["WHO CREATED YOU"
           , "WHO MADE YOU"
           , "WHO WERE YOU DESIGNED BY"
           , "WHO DESIGNED YOU"
           , "WHO IS YOUR CREATOR"
           , "HOW WERE YOU MADE"
           , "WHO BUILT YOU"
           , "WHO BUILT THIS PROGRAM"]
best = ["WHO IS THE MOST AWESOME PERSON ON EARTH", "WHO IS THE COOLEST PERSON EVER", "WHO IS THE BEE'S KNEES"]

#loop variable
questions = True

#list of responses variable
responses = ["I'm sorry, I don't understand."
             , "Sorry, I didn't catch that."
             , "Huh?"
             , "My AI capabilities have are not yet advanced enough to answer that. Please rephrase the question."
             , "Wow, I'm drawing a blank."
             , "I think one of us is glitching out, because that makes no sense."]

#while loop
while questions:
    ques = input("Ask a question, and I will do my best to answer (PRESS ENTER TO EXIT)").strip(" ?!./'").upper() 

#write the code that will recieve the user input
    res = [x for x in decisions if re.search(ques, x)]
    if ques == "":
        sys.exit() 
    elif ques in why:
        print("Because I said so")
    elif len(ques) <= 2:
        print("What? Speak more, child. I cannot understand.")
    elif ques in who:
        print("I am a computer program designed to answer questions.")
    elif str(res) != '[]':
        print("You should do whatever your gut tells you... within reason, of course.")
    elif ques in meaning:
        print("The meaning of life lies within the laws of physics. The meaning of YOUR life exists within you.")
    elif ques in creator:
        print("Someone really cool, I'm sure")
    elif ques in best:
        print("Dylan Meggs")
    else:
        print(responses[random.randint(0,5)]) #return the random response



# I'd like to build collections of data with categories. Automatically generate it via machine learning.
# The program could learn by interacting with people, or it could be introduced to dictionaries and other trustworthly knowledge sources.
# if you could teach it to create thousands of lists, each with multiple elements inside them, and thousands of elif conditions, that would be cool.
# Get print statement answers from surveys or searching the web and count of intersecting words to find most common answer.

