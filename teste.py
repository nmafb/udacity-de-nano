

temperature = 30

if temperature > 30:
    print("Its a hot day")
elif temperature > 20:
    print("Its a nice day")
elif temperature <= 20:
    print("Its a cold day")

print('Done')

i = 1
while i <= 10:
    print(i * '*')
    i = i+1


names = ["John","Bob", "Mosh", "Sara", "Maria"]
print(names)
print(names[0]) # First Element
print(names[-1]) # Last Element
print(names[-2]) # Second Element from the Last Element

names[0] = "bananas"
print(names)
print(names[0:3]) #excludes the End Index

# Lists
numbers = [1,2,3,4,5]
numbers.append(6)
print(numbers)
numbers.insert(0,-1)
print(numbers)
numbers.remove(3)
print(numbers)

print(1 in numbers)
print(len(numbers))


numbers.clear()
print(numbers)

numbers_1 = [1,2,3,4,5]
for item in numbers_1:
    print(item)

print ("-----------")

i = 0
while i < len(numbers_1):
    print(numbers_1[i])
    i = i +1


