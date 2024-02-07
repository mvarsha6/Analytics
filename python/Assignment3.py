#Tuples and Strings

x=(1,2,3,1,5,4,6,7,8,9,10,9)
#Question-1
'''Write a Python program to get the 4th element from the last element of a tuple.'''
print(x[-4])

#Question-2
'''Write a Python program to find repeated items in a tuple'''
duplicates=[]
for i in x:
    if x.count(i)>=2:
        if i not in duplicates:
            duplicates.append(i)
print(duplicates)

#Question-3
'''Write a Python program to check whether an element exists within a tuple. 
Ask something from user, if that exists in tuple, then print “YES” else print “NO”.'''
n=5
if n in x:
    print("Yes")
else:
    print("No")

#Question-4
'''Write a Python program to reverse a tuple.'''
x_rev=x[::-1]
print(x_rev)

#Question-5
'''Write a Python program to check if a string has at least one letter and one number. 
If it has at least one letter and one number then print YES else print NO.
a=97 z=122, A=65, Z=90, 0=48 9=57 '''
y=("The World i5 Beautifu1")
letter_count=0
digit_count=0
for i in range(len(y)):
    if ord(y[i])>=97 and ord(y[i])<=122:
        letter_count+=1
    elif ord(y[i])>=65 and ord(y[i])<=90:
        letter_count+=1
    elif ord(y[i])>=48 and ord(y[i])<=57:
        digit_count+=1
if letter_count>0 and digit_count>0:
    print("Question-5: Yes")
else:
    print("Question-5: No")

#Question-6
'''Write a python program to ask a string from user. 
Then count the number of vowels and number of consonants in that string.'''
y=("The World i5 Beautifu1")
vow_count=0
cons_count=0
for i in range(len(y)):
    if y[i] in ["a","e","i","o","u"]:
        vow_count+=1
    elif y[i] in ["A","E","I","O","U"]:
        vow_count+=1
    else:
        cons_count+=1
print(f'{cons_count=}, {vow_count=}')

#Question-8
'''Ask a string from user. Print the string with first 2 letters and last 2 letters.'''
str_letters=[]
for i in range(len(y)):
    if ord(y[i])>=97 and ord(y[i])<=122:
        str_letters.append(y[i])
    elif ord(y[i])>=65 and ord(y[i])<=90:
        str_letters.append(y[i])
print(str_letters)
print(f'{str_letters[0:2]=}, {str_letters[-2: :1]=}')

#Question-9
'''Write a python program to only print second half of the string. Ask string from user.'''
str_2ndhalf=y[(len(y)//2):]
print(f'{str_2ndhalf=}')

#Question-10
'''Ask a string from user. Print how the count of alphabets (a-z or A-Z) in that string.'''
alpha_count=0
for i in range(len(y)):
    if ord(y[i])>=97 and ord(y[i])<=122:
        alpha_count+=1
    elif ord(y[i])>=65 and ord(y[i])<=90:
        alpha_count+=1
print(f'Question-10, count={alpha_count}')
