# --------------------------------------------------------------------------------
# SPADE - Support for Provenance Auditing in Distributed Environments.
# Copyright (C) 2015 SRI International
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------------------------


def getDictionnary(path=None):
    if(path == None):
        #hypertension = Amlovas
        return {
    'I':['infection', 'unstable', 'bleed', 'critic',
         'antiemeti', 'antipyreti', 'analgeti', 'diureti',
         'dvt',
          ['wound', 'infect'], 'atrial', ['mental', 'confusi'], #samrat
          'diarrh', 'unstable', ' aki',#samrat
          'af ','atelectasis', ['transient','confusion']##table from samrat
         ],
    'II':[' transfus', 'prbc',
          'hemorrha', 'haemorrha', 
          'antibio', 'zosyn', 'octreotid', 'colistin',
          #samrat
          ['chest', 'infect'],['ryles', 'tube', 'insert'], ['ischemic', 'attack'],
          ['infectious', 'diarrh'], ['urinar', 'tract', 'infect'],
          #'ivf'#intravenous fluid
          'tachyarrhy', 'pneumoni', 'tia ', ' tia', ['anti', 'coagul'], ['diarrh', 'antibio'], 'uti', ['nb', 'tube']  ##table from samrat
          ],
    'IIIa':['intervent', 
            'usg', 'ncct', #exams
            #samrat
            'pacemaker', 'anaesthesi', #local VS general
            'bradyarrhythmi', 'anest', 'biloma', ['percutaneous', 'drain'], ['dehiscen', 'wound'] ##table from samrat
            ],
    'IIIb':[' redo', 'redo ', 'rexplor',
           ['redo', 'laparoto'], ['rexplo', 'laparato'], 'eventrat' ##table from samrat
           ],
    'IVa':[' dialysis ', ' hemorrhage ', ' stroke ', 'failure', 'perforation', 
           'caridac arrest', ['cardiac', ' mi'],
           'digoxin',#medicine again heart failure
           'necro',
           ['bowel', 'reconn'],
           ' icu ', ' icu',
           ['nasal', 'prong'],
          'ischemic'            ##table from samrat
           ],
    'IVb':['cmv',
           'mods'##table from samrat
           ],
    'V':['death','duef','died', 'expire', 'dead']
        }
        
    else:
        f = open(path, 'r')
        f.readline()
        d = {}
        for line in f.readlines():
            cas = line.split(',')
            listres = []
            for word in cas[1:]:
                word = word.replace('\n','')
                if(len(word)>0):
                    if(';' in word):
                        listres.append(word.split(';'))
                    else:
                        listres.append(word)
            d[cas[0]] = listres
        f.close()
        return d
    

def exportDictionnary(d, path='/Users/coulson/rxc/sri_raxa/source/splitting by POD/resultDir/testdic.csv'):
     f = open(path, 'w')
     f.write('CD,words\n')
     for k,v in d.items():
        f.write(k+',') #TODO
        for word in v:
            if(type(word)==str):
                f.write(word+',')
            else:
                f.write(';'.join(word)+',')
        f.write('\n')  
     f.close()
      