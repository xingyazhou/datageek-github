# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 15:20:36 2020

@author: xingy
"""

def getPackages(line, keyword):
   start = line.find(keyword) + len(keyword)
   #print(line.find(keyword))
   #print(start);

   end = line.find(';', start)
   #rint(end)

   if start < end:
      packageName = line[start:end].strip()
      #print(packageName);
      return splitPackageName(packageName)
      #return packageName
   else:
      return []

def splitPackageName(packageName):
   """e.g. given com.example.appname.library.widgetname
           returns com
	           com.example
                   com.example.appname
      etc.
   """
   print(packageName);
   result = []
   end = packageName.find('.')
   while end > 0:
      #print(end);
      result.append(packageName[0:end])
      end = packageName.find('.', end+1)
   result.append(packageName)
   print(result)
   return result



text = "import org.apache.beam.sdk.Pipeline;";
keyword = "import"

x=getPackages(text, keyword);

#print(x)
