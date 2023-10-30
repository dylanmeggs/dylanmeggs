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

"""
Side thought... Google search engine algorithm
Visit every registered website, one by one
Save a copy of html
Use something similar to this chat bot to find correlations and determine 
what websites a user might be looking for
Use statistics to help by counting words, phrases, and any website tags 
added in the background for SEO purposes
Use other KPIs like # of site visits, location, and the history of the 
person searching
Get a list of top x number of sites based on the first set of conditions
Order the list based on a second set of conditions
How does Google do this all so quickly? MapReduce and PageRank - used to 
generate an efficient index of the WWW; frequently refreshed
PageRank is one of several algortithms/calculations used to rank pages. A 
page rank counts the number of times other webistes link to another.
It also considers how 'important' the other websites linking to it are. 
This is done by considering their page rank. Additionally, it considers
how many times people jump from those other websites to the one being 
ranked at the time of calculation.
MapReduce is a programming model that is basically just all the code 
running to calculate an output. The description of MapReduce on wiki also
mentioned clusters and running multiple processes at the same time.
TLDR:
Ingest HTML from entire WWW
Simultaneously run a variety of conditions/arguments to filter/reduce the 
data
Simultaneously run a variety of calculations to rank websites prior to a 
search being made
Spit it all out into a giant index on a database
Customer searches something
The same process happens, just with different criteria and calculations
It's on an index that's already halfway sorted, and google has a ton of 
servers, so the search comes back quickly
Also... google probably stores thousands of indexes linked to common 
searches
If your question is common or easily predictable, it can give an answer 
without running any calculations.
It reads "go to row with key = [whatever was written in google search 
bar], if this does not exist, run calculation"
These indexes all refresh 'frequently'
"""