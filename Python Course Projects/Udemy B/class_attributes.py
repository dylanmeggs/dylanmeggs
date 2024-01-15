# Name of an employee should not be used as a class attribute
# You should use something that is common among people within the company
class Employee:
    numberOfWorkingHours = 40

#Create two objects
employeeOne = Employee()
employeeTwo = Employee()
employeeOne.numberOfWorkingHours
employeeTwo.numberOfWorkingHours

#Point to class to change attribute values
Employee.numberOfWorkingHours = 45
employeeOne.numberOfWorkingHours
employeeTwo.numberOfWorkingHours

employeeOne.name = "Dylan"
employeeOne.name
employeeTwo.name #AttributeError: 'Employee' object has no attribute 'name'
employeeTwo.name = 'Mary'
employeeTwo.name
employeeOne.numberOfWorkingHours = 40
employeeOne.numberOfWorkingHours
#The class seems to create a default value for any object built from the class. 
#You can add/edit attributes for each object later.

