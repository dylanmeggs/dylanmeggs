# Create a more intricate Magic 8 Ball using classes
# Export the questions to csv

import sys
import random
import csv

class magic8ball:
    def __init__(self, name):
        self.__name = name #properties of init method
        self.__mQuestions = []
        self.__start_game() #initializing a class calls the start game function

    def __start_game(self):
        responses = ["IT IS CERTAIN", "YOU MAY RELY ON IT", "AS I SEE IT, YES", "OUTLOOK LOOKS GOOD", "MOST LIKELY", "IT IS DECIDELY SO", "WITHOUT A DOUBT", "YES, DEFINETLY"]
        questions = True
        print("Welcome " + self.__name)

        while questions:
            ques = input("Please ask a question (press enter to exit): ")
            if ques == "":
                print("Game ended.")
                self.__write_questions()
                sys.exit()
            else:
                print(responses[random.randint(0,7)])
                self.__mQuestions.append(ques)
# self.__write_questions() adding this here causes duplications. It prints the whole list, so each time you ask a new question, it also writes the old ones again.
# exiting the program (via enter or any other method) causes it to print the whole list again since I have it included in the first if statement

    def __write_questions(self):
        file = open("magic_questions.csv", "a", newline="")
        wrt = csv.writer(file) # wrt is new variable = the csv writer function
        
        for x in self.__mQuestions:
            wrt.writerow([x])
        file.close()