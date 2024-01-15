type(1)
2**4 #2 ^ 4
4 % 2 #mod which returns remainder of the division
(2 + 3) * (5 + 5) #Order of operations applies
x = 1
y = 2
x + y
'double'
"I don't like that"
print('double')
print("First: {x} Second: {y}".format(x='XXX', y='YYY'))
[1, 'a']
mylist = ['a', 'b', 'c']
#Do an index call to pull from list
mylist[0] #first
mylist[-1] #go backwars in the list and pull last item
mylist[0] = 'NEWA'
mylist.append('d')
mylist
newlist = [1,2,[10,20]] #List inside of a list
newlist[2][0] #Pull from nested list. Similar to pulling from matrices later with spark
d = {'key1':'value1', 'key2':'value2'} #dictionary with key-value pairs
d['key1'] #No order is retained in dictionaries. Hash table.
True #1
False #0
t = (1,2,3) #tuple, cannot reassign the items to new values
s = {1,2,3} #Set, unordered list of elements. No duplicates.
s2 = {1,1,2,3}
s2 #it automatically removed duplicates

i = 1
while i < 5:
    print('i is {}'.format(i))
    i = i+1
for element in t:
    print('hello')

fast_list = list(range(10) #quickly create a list
i = list(range(2,50,2)) #start,stop,step
i
for x in i:
    print('available')

#creating lists
x = [1,2,3,4,5]
out = []
for num in x:
    out.append(num**2)

print(out)

#convert your for loop into list comprehension
[num**2 for num in x]
# This is the same as above. It is less readable for newcomers, but faster to write. It has slight performance boost too.

# Up next, dataframes. And once you know that you'll move to Dataframe MLlib API for machine learning