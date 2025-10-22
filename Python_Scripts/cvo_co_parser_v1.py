'''hfshdfjhsaD
SJDFKSJDF
KSDJFKLSd
]SDJFKKSDF'''
def cvo_co_get_file_v1(fn,np):
   import csv
   from time import mktime, strptime
   import numpy as np
    
   DT = []
   ET = []
   DoY = []
   CO = []
   flag = []
   
   ifile = open(fn, 'rU')
   reader = csv.reader(ifile, delimiter = chr(9)) #9 = tab, 44 = ,
   for row in reader:
      xx = str(row[0])
      ix = xx.find("Datetime")
      if ix < 0:
         xx = str(row[0])
         tt = strptime(str(row[0]), '%d/%m/%y %H:%M')
         #DoY
         DoY.append(float(tt[7]) + ((((float(tt[5])/60) + float(tt[4]))/60) + float(tt[3]))/24) 
         #ET
         ET.append(int(mktime(tt)))
         #DT
         DT.append(tt[0:6])
         #o2/n2 ratio
         xx = str(row[1])
         CO.append(float(xx))
         #fl
         xx = str(row[2])
         flag.append(int(xx))
		 
   return np.array(DT), np.array(DoY), np.array(ET), np.array(CO), np.array(flag) 
   
def cvo_co_parse_data_v1(data):
   ix = len(data.DoY)
   for ii in range(0,len(data.DoY)):
      if data.DT[ii,1] != data.DT[0,1]:
         ix = ii
		 
   return data.DT[0:ix,:], data.DoY[0:ix], data.ET[0:ix], data.CO[0:ix], data.flag[0:ix]