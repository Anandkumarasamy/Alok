
def minion_game(string):
    vowels = "AaEeIiOoUu"
    set_lis=set(list(string))
    stuart_lis=[]
    kevin_lis=[]
    stuart_count=0
    kevin_count=0
    for char in set_lis:
        if char in vowels:
            stuart_lis.append(char)
        else:
            kevin_lis.append(char)
    for stuart_char in stuart_lis:
        ind=0
        while ind == len(string):
        
        

if __name__ == '__main__':
    s = input()
    minion_game(s)