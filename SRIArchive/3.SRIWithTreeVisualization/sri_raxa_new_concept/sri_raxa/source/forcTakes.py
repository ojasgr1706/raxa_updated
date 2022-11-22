#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 14:56:03 2017

@author: study
"""


from bs4 import BeautifulSoup
import json


def get_extracted_concepts(ctakes_doc_content, doc):
    ctakes_doc = BeautifulSoup(ctakes_doc_content, 'xml')

    umls_concepts = []

    for cas_FSArray in ctakes_doc.find_all('uima.cas.FSArray'):

        matching_ent = ctakes_doc.find(
            attrs={'_ref_ontologyConceptArr': cas_FSArray.attrs['_id']}
        )

        if matching_ent is None:
            continue
        classe = matching_ent.name[42:]
        start = int(matching_ent.attrs['begin'])
        end = int(matching_ent.attrs['end'])
        ngram = doc[start:end]

        for i, umls_id in enumerate(cas_FSArray.find_all('i'), start=1):
            umls_concept_soup = ctakes_doc.find(
                'org.apache.ctakes.typesystem.type.refsem.UmlsConcept',
                attrs={'_id': umls_id.text})

            if umls_concept_soup is None:
                break

            score = (
                float(umls_concept_soup.attrs['score'])
                if float(umls_concept_soup.attrs['score']) > 0
                else 1 / i
            )

            extracted = {
                'class': classe,
                'similarity': score,
                'cui': umls_concept_soup.attrs['cui'],
                'semtypes': [umls_concept_soup.attrs['tui']],
                'term': umls_concept_soup.attrs.get('preferredText', ngram),
                'subject': matching_ent.attrs['subject']
            }
            umls_concepts.append(extracted)

    return umls_concepts


def get_extracted_concepts_robin(pathRes, withWord=False):
    ctak = open(pathRes, 'r')
    textOrigin = None
    dicOnotology = {}
    dicConcept = {}
    go = False
    res = []
    for l in ctak.readlines():
        if (withWord and textOrigin == None and "uima.cas.Sofa" in l):
            textOrigin = get_sub(l, 'sofaString', False)
        if ("_ref_ontologyConceptArr" in l):
            ref = get_sub(l, "_ref_ontologyConceptArr")
            begin = get_sub(l, "begin", True)
            end = get_sub(l, "end", True)
            classC = l[:l.index("_") - 1].split('.')[-1]
            if (ref in dicOnotology):
                print('ERROR : conflict in _ref_ontologyConceptArr')
            dicOnotology[ref] = (begin, end, classC)

        if (go != None):
            if ('<i>' in l):
                _id = l[l.index('<i>') + len('<i>'):l.index('</')]
                if (_id in dicConcept):
                    if (withWord):
                        word = textOrigin[dicOnotology[go][0]:dicOnotology[go][1]]
                        if ([word, dicConcept[_id], dicOnotology[go][2]] not in res):
                            res.append([word, dicConcept[_id], dicOnotology[go][2]])
                    else:
                        if ([dicOnotology[go][2], dicConcept[_id]] not in res):
                            res.append([dicOnotology[go][2], dicConcept[_id]])

        if ('uima.cas.FSArray' in l and '/' not in l):
            go = get_sub(l, '_id')
        if ('/uima.cas.FSArray' in l):
            go = None

        if ('org.apache.ctakes.typesystem.type.refsem.UmlsConcept' in l):
            _id = get_sub(l, '_id')
            txt = get_sub(l, 'preferredText')
            if (_id in dicConcept):
                print('ERROR : conflict in umls concept ids')
            dicConcept[_id] = txt
    ctak.close()
    return res

def get_extracted_concepts_new_ctakes(ctakes_data):
    res = []
    mentionAnnotationType = ["DrugChangeStatusAnnotation", "LabValueMentionList", "StrengthAnnotation","TemporalTextRelationsList"
                             "DiseaseDisorderMention", "SignSymptomMention", "DrugNerMentionList", "ProcedureMention"]

    resWithoutDuplicates = []
    for annotation in ctakes_data:
        if annotation in mentionAnnotationType:
            for value in ctakes_data[annotation]:
                print(value)
                try:
                    preferredText = json.loads(value)
                    print(preferredText['labName'])
                    res.append(preferredText['labName'])
                except Exception as e:
                    print(e)

    for tempRes in res:
        if tempRes not in resWithoutDuplicates:
            resWithoutDuplicates.append(tempRes)
    return resWithoutDuplicates



def get_extracted_concepts_chahal(responseText, withWord=False):
    print("inside get extracted concepts chahal")
    res = []
    resWithoutDuplicates = []
    responseJson = json.loads(responseText)
    mentionType = ["org.apache.ctakes.typesystem.type.textsem.ProcedureMention",
                   "org.apache.ctakes.typesystem.type.textsem.DiseaseDisorderMention",
                   "org.apache.ctakes.typesystem.type.textsem.MedicationMention",
                   "org.apache.ctakes.typesystem.type.textsem.SignSymptomMention",
                   "org.apache.ctakes.typesystem.type.textsem.AnatomicalSiteMention",
                   "org.apache.ctakes.typesystem.type.textsem.EntityMention",
                   "org.apache.ctakes.typesystem.type.textsem.EventMention",
                   "org.apache.ctakes.typesystem.type.textsem.LabMention",
                   "org.apache.ctakes.typesystem.type.textsem.MedicationEventMention",
                   "org.apache.ctakes.typesystem.type.textsem.TimeMention"]

    for mentionList in responseJson:
        if mentionList['typ'] in mentionType:
            ontologyConceptArr = mentionList["annotation"]["ontologyConceptArr"]
            for umlsConcept in ontologyConceptArr:
                preferredText = umlsConcept["annotation"]["preferredText"]
                res.append([preferredText])

    for tempRes in res:
        if tempRes not in resWithoutDuplicates:
            resWithoutDuplicates.append(tempRes)

    return resWithoutDuplicates


def get_sub(l, ref, toInt=False, lenght=None):
    if (lenght == None):
        res = l[l.index(ref) + len(ref) + 2:]
        if (toInt):
            return int(res[:res.index('"')])
        else:
            return res[:res.index('"')].replace('&#10;', '\n')
    else:
        return l[l.index(ref) + len(ref):l.index(ref) + len(ref) + lenght]


def filterClassTerm(listDic):
    res = []
    res.append(['class', 'term'])
    for d in listDic:
        if ([d['class'], d['term']] not in res):
            res.append([d['class'], d['term']])
    return res


def getUnique(lis):
    res = [lis[0]]
    last = lis[0]
    for i in lis[1:]:
        if (i != last):
            last = i
            res.append(i)
    return res


def postcTakes(path, file):
    res = get_extracted_concepts_robin(path + 'apres/' + file + '.xml')
    return res
