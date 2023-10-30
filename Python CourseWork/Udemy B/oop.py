# Object oriented programming. 
# Classes and functions help us give certain groups access to certain features or actions
# Classes are a logical collection of attributes and methods. Objects are extensions of classes. 

class Employee:
    name = "Dylan"
    designation = "Business Intelligence Engineer"
    level = "L5"
    ticketsCompletedThisWeek = 6
    #These are attributes
    def hasAchievedTarget(self):
            if self.ticketsCompletedThisWeek >= 5:
                print("Target has been achieved")
            else:
                print("Target has not been achieved")

employeeOne = Employee()
employeeOne.name
employeeOne.hasAchievedTarget()