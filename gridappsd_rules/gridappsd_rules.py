# -------------------------------------------------------------------------------
# Copyright (c) 2018, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# -------------------------------------------------------------------------------
import getpass
from SPARQLWrapper import SPARQLWrapper2


if getpass.getuser() == 'root' or getpass.getuser() == 'gridappsd':  # Docker check
    sparql = SPARQLWrapper2("http://blazegraph:8080/bigdata/sparql")
else:
    sparql = SPARQLWrapper2("http://127.0.0.1:8889/bigdata/sparql")

def FlatPhases (phases):
    if len(phases) < 1:
        return ['A', 'B', 'C']
    if 'ABC' in phases:
        return ['A', 'B', 'C']
    if 'AB' in phases:
        return ['A', 'B']
    if 'AC' in phases:
        return ['A', 'C']
    if 'BC' in phases:
        return ['B', 'C']
    if 'A' in phases:
        return ['A']
    if 'B' in phases:
        return ['B']
    if 'C' in phases:
        return ['C']
    if 's12' in phases:
        return ['s12']
    if 's1s2' in phases:
        return ['s1', 's2']
    if 's1' in phases:
        return ['s1']
    if 's2' in phases:
        return ['s2']
    return []

prefix17 = '''
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/2012/CIM-schema-cim17#>
    '''

prefix16 = '''
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/2012/CIM-schema-cim16#>
    '''

feeders = { u'ieee123': u'_C1C3E687-6FFD-C753-582B-632A27E28507',
            u'ieee8500': u'_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3',
            u'j1': u'_67AB291F-DCCD-31B7-B499-338206B9828F',
            u'r2_12_47_2': u'_9CE150A8-8CC5-A0F9-B67E-BBD8C79D3095'}

feeder_name = feeders['ieee8500']
# feeder_name = feeders['ieee123']

def lookup_meas_trmid(value='_B07DE616-065C-B78F-5F9E-A67BEFC06D7C', feeder=u'_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'):
    # list all measurements, with buses and equipments - DistMeasurement
    query = '''PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c: <http://iec.ch/TC57/2012/CIM-schema-cim17#>
    SELECT ?class ?type ?name ?bus ?phases ?eqtype ?eqname ?eqid ?trmid ?id ?ce WHERE {
     VALUES ?fdrid {"''' + feeder + '''"}
     VALUES ?trmid {"''' + value + '''"}
     ?eq c:Equipment.EquipmentContainer ?fdr.
     ?fdr c:IdentifiedObject.mRID ?fdrid. 
    { ?s r:type c:Discrete. bind ("Discrete" as ?class)}
      UNION
    { ?s r:type c:Analog. bind ("Analog" as ?class)}
     ?s c:IdentifiedObject.name ?name .
     ?s c:IdentifiedObject.mRID ?id .
     ?s c:Measurement.PowerSystemResource ?eq .
     ?s c:Measurement.Terminal ?trm .

     ?trm c:Terminal.ConductingEquipment ?ce.
     #?s c:ConductingEquipment.BaseVoltage ?lev. 
     #?lev c:BaseVoltage.nominalVoltage ?vnom1.

     ?s c:Measurement.measurementType ?type .
     ?trm c:IdentifiedObject.mRID ?trmid.
     ?eq c:IdentifiedObject.mRID ?eqid.
     ?eq c:IdentifiedObject.name ?eqname.
     ?eq r:type ?typeraw.
      bind(strafter(str(?typeraw),"#") as ?eqtype)
     ?trm c:Terminal.ConnectivityNode ?cn.
     ?cn c:IdentifiedObject.name ?bus.
     ?s c:Measurement.phases ?phsraw .
       {bind(strafter(str(?phsraw),"PhaseCode.") as ?phases)}

    } ORDER BY ?class ?type ?name
    '''
    sparql.setQuery(query)
    result = sparql.query()
    # print ret.bindings
    ret = {}
    for b in result.bindings:
        #         print (b['bus'].value, b['phases'].value, b['eqtype'].value, b['eqname'].value, b['eqid'].value, b['trmid'].value )
        ret[b['id'].value] = {'name': b['name'].value, 'type': b['type'].value, 'phases': b['phases'].value,
                                 'trmid': b['trmid'].value, 'bus': b['bus'].value, 'ce': b['ce'].value,
                                 'eqid': b['eqid'].value}
    return ret

def lookup_reg(feeder =u'_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'):
    fidselect = """ VALUES ?fdrid {\"""" + feeder + """\"}
    ?s c:Equipment.EquipmentContainer ?fdr.
    ?fdr c:IdentifiedObject.mRID ?fdrid. """
    descrete_query = """
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/2012/CIM-schema-cim17#>
    SELECT ?name ?wnum ?bus (group_concat(distinct ?phs;separator=\"\") as ?phases) ?eqid ?trmid ?vnom WHERE {
     SELECT ?name ?wnum ?bus ?phs ?eqid ?trmid ?vnom WHERE {  """ + fidselect + """
     ?rtc r:type c:RatioTapChanger.
     ?rtc c:IdentifiedObject.name ?rname.
     ?rtc c:IdentifiedObject.mRID ?rtcid.
     ?rtc c:RatioTapChanger.TransformerEnd ?end.
     ?end c:TransformerEnd.endNumber ?wnum.
     ?end c:TransformerEnd.Terminal ?trm.
     ?trm c:IdentifiedObject.mRID ?trmid. 
     ?trm c:Terminal.ConnectivityNode ?cn. 
     ?cn c:IdentifiedObject.name ?bus.
     ?end c:TransformerEnd.BaseVoltage ?tev. 
     ?tev c:BaseVoltage.nominalVoltage ?vnom.
     OPTIONAL {?end c:TransformerTankEnd.phases ?phsraw.
      bind(strafter(str(?phsraw),"PhaseCode.") as ?phs)}
     ?end c:TransformerTankEnd.TransformerTank ?tank.
     ?tank c:TransformerTank.PowerTransformer ?s.
     ?s c:IdentifiedObject.name ?name.
     ?s c:IdentifiedObject.mRID ?eqid.
     ?tank c:IdentifiedObject.name ?tname.
     } ORDER BY ?name ?phs
    }
    GROUP BY ?name ?wnum ?bus ?eqid ?trmid ?vnom
    ORDER BY ?name"""
    sparql.setQuery(descrete_query)
    result = sparql.query()
    ret = {}
    for b in result.bindings:
        phases = FlatPhases(b['phases'].value)
        for phs in phases:
            # print ('PowerTransformer','RatioTapChanger',b['name'].value,b['wnum'].value,'bus', b['bus'].value,phs,b['eqid'].value,'trmid',b['trmid'].value, b['vnom'].value)
            ret[b['eqid'].value] = {'name': b['name'].value, 'phases': phs,
                                  'trmid': b['trmid'].value, 'bus': b['bus'].value,
                                  'eqid': b['eqid'].value}

    return ret

def get_line_segements(value='_67AB291F-DCCD-31B7-B499-338206B9828F'):
    fidselect = """ VALUES ?fdrid {\"""" + value + """\"}
                     ?s c:Equipment.EquipmentContainer ?fdr.
                     ?fdr c:IdentifiedObject.mRID ?fdrid. """

    query = prefix17 + """SELECT ?name ?bus1 ?bus2 (group_concat(distinct ?phs;separator=\"\") as ?phases) ?eqid ?trm1id ?trm2id ?vnom1 WHERE {
     SELECT ?name ?bus1 ?bus2 ?phs ?eqid ?trm1id ?trm2id ?vnom1 WHERE {""" + fidselect + """
     ?s r:type c:ACLineSegment.
     ?s c:IdentifiedObject.name ?name.
     ?s c:IdentifiedObject.mRID ?eqid. 
     ?t1 c:Terminal.ConductingEquipment ?s.
     ?s c:ConductingEquipment.BaseVoltage ?lev. 
     ?lev c:BaseVoltage.nominalVoltage ?vnom1.
     ?t1 c:ACDCTerminal.sequenceNumber "1".
     ?t1 c:IdentifiedObject.mRID ?trm1id. 
     ?t1 c:Terminal.ConnectivityNode ?cn1. 
     ?cn1 c:IdentifiedObject.name ?bus1.
     ?t2 c:Terminal.ConductingEquipment ?s.
     ?t2 c:ACDCTerminal.sequenceNumber "2".
     ?t2 c:IdentifiedObject.mRID ?trm2id. 
     ?t2 c:Terminal.ConnectivityNode ?cn2. 
     ?cn2 c:IdentifiedObject.name ?bus2.
     OPTIONAL {?acp c:ACLineSegmentPhase.ACLineSegment ?s.
     ?acp c:ACLineSegmentPhase.phase ?phsraw.
        bind(strafter(str(?phsraw),\"SinglePhaseKind.\") as ?phs) } } ORDER BY ?name ?phs
     } GROUP BY ?name ?bus1 ?bus2 ?eqid ?trm1id ?trm2id ?vnom1
     ORDER BY ?name
    """
    ret = {}
    sparql.setQuery(query)
    result = sparql.query()
    for b in result.bindings:
        # print (b['name'].value, b['bus1'].value, b['bus2'].value, b['eqid'].value, b['trm1id'].value, b['trm2id'].value, b['vnom1'].value)
        ret[b['eqid'].value] = {'name': b['name'].value, 'vnom1': b['vnom1'].value, 'eqid' : b['eqid'].value,
                                'phases' :b['phases'].value, 'trm1id': b['trm1id'].value, 'trm2id': b['trm2id'].value,
                                'bus1': b['bus1'].value, 'bus2': b['bus2'].value}
    return ret


def get_regulator():
    reg_res = lookup_reg()
    connected_lines1 = {}
    lines = get_line_segements(feeder_name)
    regulator_buses = [x['bus'] for x in reg_res.itervalues()]
    for reg_bus in set(regulator_buses):
        # lines = lookup_meas_trmid(reg['trmid'], feeder_name)
        for line in lines.values():
            if reg_bus == line['bus1']:
                line_meas = lookup_meas_trmid(line['trm1id'])
                for k, meas in line_meas.items():
                    # meas['bus'] = reg_bus
                    meas['base_voltage'] = line['vnom1']
                    connected_lines1[k] = meas


    # regulator_buses = [x['bus'] for x in reg_res.itervalues()]
    # connected_lines = {}
    # lines = lookup_meas_name('ACLineSegment', feeder_name)
    # for reg_bus in set(regulator_buses):
    #     for k, line in lines.items():
    #         if reg_bus == line['bus']:
    #             # print (k, line)
    #             connected_lines[k] = line
    return reg_res, connected_lines1



def get_regulator_meas():
    reg_res, connected_lines = get_regulator()
    return connected_lines


