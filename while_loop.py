# write a program that reads a number N and prints the numbers 0 N times on N lines.
n= int(input())
c1=1
while c1<n+1:
    c2=0
    while c2<n+1:
        print(c2,end=' ')
        c2+=1
    print()
    c1+=1
