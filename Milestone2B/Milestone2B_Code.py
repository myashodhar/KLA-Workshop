import pandas as pd
import yaml
import time
from datetime import datetime
import threading

f = open("Milestone2B_Log.txt", "w")

task_dict = {}
with open('Milestone2B.yaml') as file:
    tasks = yaml.safe_load_all(file)
    
    for task in tasks:
        task_dict.update(task)


f.write(f"{datetime.now()};M2B_Workflow Entry \n")
main_f = task_dict['M2B_Workflow']

def TimeFunction(inpts,path,task,condition):
    lock.acquire()
    if(len(condition)>0):
        i = 1
        if(condition[len(condition)-2] == '1'):
            i=2
        s=condition[2:len(condition)-4-i]
        
        if(condition[len(condition)-2] == '1'):
            num = eval(condition[len(condition)-2]+condition[len(condition)-1])
        else:
            num = eval(condition[len(condition)-1])
        if(condition[len(condition)-i-2] == '>'):
            
            if RD[s]>num:
                pass
            else:
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                lock.release()
                return None
            
        elif condition[len(condition)-2-i] == '<':
            if(RD[s]<num):
                pass
            else:
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                lock.release()
                return None
    f.write(f"{datetime.now()};{path} Executing TimeFunction ({inpts['FunctionInput']},{inpts['ExecutionTime']}) \n")
    print("executing ......")
    time.sleep(int(inpts['ExecutionTime']))
    print("done...")
    if(len(task) == 0):
        f.write(f"{datetime.now()};{path} Exit \n")
    else:
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
    lock.release()

def DataLoad(inpts,path,task,condition):
    lock.acquire()
    if(len(condition)>0):
        i = 1
        if(condition[len(condition)-2] == '1'):
            i=2
        s=condition[2:len(condition)-4-i]
        
        if(condition[len(condition)-2] == '1'):
            num = eval(condition[len(condition)-2]+condition[len(condition)-1] )
        else:
            num = eval(condition[len(condition)-1])
            
        if(condition[len(condition)-2-i] == '>'):
            
            if RD[s]>num:
                pass
            else:
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                lock.release()
                return None
            
        elif condition[len(condition)-2-i] == '<':
            if(RD[s]<num):
                pass
            else:
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                lock.release()
                return None
            
    f.write(f"{datetime.now()};{path} Executing DataLoad ({inpts['Filename']}) \n")
    df = pd.read_csv(f"{inpts['Filename']}")
    if(len(task) == 0):
        f.write(f"{datetime.now()};{path} Exit \n")
    else:
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
    print("done.....")
    lock.release()
    
    return df,len(df)
    

lock = threading.Lock()

RD = {}

def Activity(act,task,path):
    
    f.write(f"{datetime.now()};{path}.{task} Entry \n")
    if(act[task]['Type'] == 'Task'):
            if act[task].get('Condition') is None:
                condition = ""
            else:
                condition = act[task]['Condition']
        
            fun = eval(act[task]['Function'])
            if act[task]['Function'] == "DataLoad" :
                RD[path+'.'+task+'.NoOfDefects'] = fun(act[task]['Inputs'],path+"."+task,"",condition)[1]
            else:
                fun(act[task]['Inputs'],path+"."+task,"",condition)
                
    elif act[task]['Type'] == 'Flow':
        if act[task]['Execution'] == 'Sequential':
                Seq(act[task],path+"."+task)
        elif act[task]['Execution'] == 'Concurrent':
                Conc(act[task],path+"."+task)
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
    

def Conc(main_f,path):
    act = main_f['Activities']
    
    thread= []
    
    for task in act:
        t = threading.Thread(target=Activity,args=(act,task,path))
        t.start()
        thread.append(t)
    
    for t in thread:
        t.join()
        

def Seq(main_f,path):
    act = main_f['Activities']

    for task in act:
        f.write(f"{datetime.now()};{path}.{task} Entry \n")

        if act[task]['Type'] == 'Task':
            if act[task].get('Condition') is None:
                condition = ""
            else:
                condition = act[task]['Condition']
                
            fun = eval(act[task]['Function'])
            if act[task]['Function'] == "DataLoad" :
                RD[path+'.'+task+'.NoOfDefects'] = fun(act[task]['Inputs'],path+"."+task,"",condition)[1]
            else:
                fun(act[task]['Inputs'],path+"."+task,"",condition)
        elif act[task]['Type'] == 'Flow':
            if act[task]['Execution'] == 'Sequential':
                Seq(act[task],path+"."+task)
            elif act[task]['Execution'] == 'Concurrent':
                Conc(act[task],path+"."+task)
            f.write(f"{datetime.now()};{path}.{task} Exit \n")
        
        

if main_f['Execution'] == 'Sequential':
    Seq(main_f,"M2B_Workflow")
    f.write(f"{datetime.now()};M2B_Workflow Exit\n")