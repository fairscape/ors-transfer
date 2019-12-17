import os
import sys
#os.execl(sys.executable, "python3", './app/uploader.py')
#os.system("python3 ./app/uploader.py &")
print('hi')

file = open('README.md','rb')
from multiprocessing import Process, Queue

def upload(f,name,folder = ''):

    #filename = get_filename(file)

    minioClient = Minio('minionas.uvadcos.io',
                    access_key='breakfast',
                    secret_key='breakfast',
                    secure=False)

    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)
    # if size == 0:
    #     print('ih')
    #     return {'upload':False,'error':"Empty File"}
    try:
           minioClient.put_object('breakfast', folder +name, f,size)

    except ResponseError as err:
           return {'upload':False}

    #f.save(secure_filename(f.filename))
    return {'upload':True,'location':'breakfast/' + folder + name}


def my_function(q, x):
    q.put(x + 100)


queue = Queue()
p = Process(target=upload, args=(file, 'README.md'))
while True:
    print('hi')
