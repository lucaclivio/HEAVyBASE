#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Name:        HeavyBaseService.py
# Purpose:     Index Server component for HeavyBase
#
# Project:     HeavyBase.py
# Purpose:     Hybrid Online-Offline multiplatform P2P data entry engine 
#              for electronic Case Report Forms and 'Omics' data sharing
#              based on a historiographical "Push-based" Peer-to-Peer DB
#
# Author:      Luca Clivio <luca.clivio@heavybase.org>
#              Contacts: 2nd mail luca@clivio.net, mobile +39-347-2538040
#
# Created:     2006/06/04
# RCS-ID:      $Id: HeavyBaseService.py $
#
# Copyright:   2006-2014 Luca Clivio
# License:     GNU-GPL v3
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Version: 5.8.5.0 - Released: 2014-02-04 12:00:00 [yyyy-mm-dd hh:mm:ss]
#-----------------------------------------------------------------------------

RELEASE="5.8.5"

import base64, zlib, datetime, time

#The server object
class HeavyBaseService:
    def __init__(self, peermode=False):
        self.peermode=peermode
        self.queue=[]
        #self.log("-","init")
        self.peers={}
        # autoelezione primario -inzio
        import random
        random.seed()
        self.id_instance=str(int(random.random()*10000000000))[0:10]
        # autoelezione primario - fine
        
    def getServiceInstance(self, project_name, username, password):
        return self.id_instance

    def log(self,project_name,string):
        #return self.client_ip
        return
        import datetime
        now = datetime.datetime.utcnow()
        ts=now.strftime("%Y%m%d%H%M%S")
        print ts+"\t"+project_name+"\t"+string
        try:
            logfile = open("HeavyBaseService_"+project_name+".log", "a")
            try:
                logfile.write(ts+"\t"+string+"\n")
            finally:
                logfile.close()
        except IOError:
            pass

    def sendMail(self, project_name, username, password, sender, comma_sep_receivers, subject, body):
        self.log(project_name,"sendMail - username="+username+"; password="+password)
        if self.peermode:
            ret = "Sending mail not allowed by peers."
        else:
            import smtplib,datetime
            receivers = comma_sep_receivers.split(",")
            message = "From: " + sender + "\n" + "To: " + comma_sep_receivers + "\n" + "Subject: " + subject + "\n\n" + body
            now = datetime.datetime.utcnow()
            ts=now.strftime("%Y%m%d%H%M%S")
            ret=""
            try:
               smtpObj = smtplib.SMTP('localhost')
               smtpObj.sendmail(sender, receivers, message)         
               ret = "Mail sent successfully. Timestamp:" + ts
            except:
               ret = "Error(2) sending mail. Timestamp:" + ts
        self.log(project_name,"sendMail - Output: "+ret)
        return ret
 
    def getRelease(self, project_name, username, password):
        self.log(project_name,"getRelease - username="+username+"; password="+password)
        if self.peermode:
            release="0.0.0"
        else:
            release=""
            if len(project_name)>0:
                if project_name=="sinpe_domus":
                    release="2.0.0"
                else:
                    release=RELEASE
        self.log(project_name,"getRelease - Output: "+release)
        return release

    def checkUpdates(self, project_name, username, password):
        import datetime
        self.log(project_name,"checkUpdates - username="+username+"; password="+password)
        if self.peermode:
            updates=""
        else:
            now = datetime.datetime.utcnow()
            ts=now.strftime("%Y%m%d%H%M%S")
            
            from HeavyBaseService_updates import HeavyBaseService_updates
            up=HeavyBaseService_updates()
            updates=up.GetFilenames(project_name)

        self.log(project_name,"checkUpdates - Output: "+updates)
        return updates

    def downloadFile(self, project_name, username, password, filename):
        import base64, zlib, datetime
        self.log(project_name,"downloadFile - username="+username+"; password="+password+"; filename="+filename)
        if self.peermode:
            encoded=""
        else:
            now = datetime.datetime.utcnow()
            ts=now.strftime("%Y%m%d%H%M%S")
            encoded=""
            try:
                infile = open(project_name+"/"+filename,"rb")
                contents=infile.read()
                infile.close()
                encoded = base64.b64encode(zlib.compress(contents))
            except:
                pass
            if encoded:
                self.log(project_name,"downloadFile - Output: <file encoded>")
            else:  
                self.log(project_name,"downloadFile - Output: <nothing>")
        return encoded

    def uploadFile(self, project_name, username, password, filename, encoded):
        import base64, zlib, datetime, os
        self.log(project_name,"uploadFile - username="+username+"; password="+password+"; filename="+filename)
        if self.peermode:
            return "File upload denied."
        else:
            try: os.makedirs("upload/"+project_name)
            except: pass
            data = zlib.decompress(base64.b64decode(encoded))
            outfile = open("upload/"+project_name+"/"+filename, 'wb')
            outfile.write(data)
            outfile.close()
            return "File "+filename+" successfully uploaded."

    def findPeers(self,projectname,username,password,id_instance, internal_ip, use_proxy, client_ip):
        import datetime
        if client_ip!="": self.peers[id_instance]=client_ip
        return str(self.peers)
        
        now = datetime.datetime.utcnow()
        ts=now.strftime("%Y%m%d%H%M%S")

        self.peers={}
        try: self.peers=pickle.load(file("peers_"+project_name+".pkl", "r"))
        except: pass

        #cleaning
        isclean=False
        while not isclean:
            idx=-1
            ct=0
            for q in self.peers:
                if (now-q["timestamp"]).seconds>180:
                    idx=ct
                    break
                ct=ct+1
            if idx!=-1:
                del self.peers[idx]
            else:
                isclean=True

        #adding current
        idx=-1
        ct=0
        #for q in self.peers:
        #    if     

    
    def formatExceptionInfo(maxTBlevel=5):
        import sys
        import traceback
        cla, exc, trbk = sys.exc_info()
        excName = cla.__name__
        try:
            excArgs = exc.__dict__["args"]
        except KeyError:
            excArgs = "<no args>"
        excTb = traceback.format_tb(trbk, maxTBlevel)
        return (excName, excArgs, excTb)

    def p2p(self, project_name, username, password, id_instance, state, partner, encoded):
        self.log(project_name,"p2p - username="+username+"; password="+password+"; id_instance="+id_instance+"; state="+state)
        import base64, zlib, datetime, time
        now = datetime.datetime.utcnow()
        ts=now.strftime("%Y%m%d%H%M%S")

        compatibilityMatrix={}
        compatibilityMatrix["42x"]=["4.2.5","4.2.6","4.2.7","4.2.8","4.2.9","4.3.0","4.3.1","4.3.2","4.3.3","4.3.4","4.3.5","4.3.6","4.3.7","4.3.8","4.3.9","4.4.0","4.4.1","4.4.2","4.4.3","4.4.4","4.4.5","4.4.6","4.4.7"]    #the keys are the compatibility groups logical code
        compatibilityMatrix["45x"]=["4.5.0","4.5.1"]    #the keys are the compatibility groups logical code
        compatibilityMatrix["452x"]=["4.5.2","4.5.3","4.5.4","4.5.5","4.5.6","4.5.7","4.5.8","4.5.9","4.6.0","4.6.1","4.6.2","4.6.3","4.6.4","4.6.5","4.6.6","4.6.7","4.6.8","4.6.9","4.7.0","4.7.1"
                                    ,"4.7.2","4.7.3","4.7.4","4.7.5","4.7.6","4.7.7","4.7.8","4.7.9","4.8.0","4.8.1","4.8.2","4.8.3","4.8.4","4.8.5","4.8.6","4.8.7","4.8.8","4.8.9","4.9.0","4.9.1"
                                    ,"4.9.2","4.9.3","4.9.4","4.9.5","4.9.6","4.9.7","4.9.8","4.9.9","5.0.0","5.0.1","5.0.2","5.0.3","5.0.4","5.0.5","5.0.6","5.0.7","5.0.8","5.0.9","5.1.0","5.1.1"
                                    ,"5.1.2","5.1.3","5.1.4","5.1.5","5.1.6","5.1.7","5.1.8","5.1.9","5.2.0","5.2.1","5.2.2","5.2.3","5.2.4","5.2.5","5.2.6","5.2.7","5.2.8","5.2.9","5.3.0","5.3.1"
                                    ,"5.3.2","5.3.3","5.3.4","5.3.5","5.3.6","5.3.7","5.3.8","5.3.9","5.4.0","5.4.1","5.4.2","5.4.3","5.4.4","5.4.5","5.4.6","5.4.7","5.4.8","5.4.9","5.5.0","5.5.1"
                                    ,"5.5.2","5.5.3","5.5.4","5.5.5","5.5.6","5.5.7","5.5.8","5.5.9","5.6.0","5.6.1","5.6.2","5.6.3","5.6.4","5.6.5","5.6.6","5.6.7","5.6.8","5.6.9","5.7.0","5.7.1"
                                    ,"5.7.2","5.7.3","5.7.4","5.7.5","5.7.6","5.7.7","5.7.8","5.7.9","5.8.0","5.8.1","5.8.2","5.8.3","5.8.4","5.8.5"]
        if project_name.find("--")<0: project_name=project_name+"--old"  #case teorically impossible
        for key in compatibilityMatrix:
            if project_name.split("--")[1] in compatibilityMatrix[key]:
                project_name=project_name.split("--")[0]+"--comp"+key
        
        returnValue=",,"+"NOP"
        try:
            #locking
            lock_ok=False
            reason_lock=""
            if len(project_name)>0 and len(username)>0 and len(password)>0:
                #while not lock_ok:
                import pickle
                try:
                    sem_datetime=pickle.load(file("sem_"+project_name+".pkl", "r"))
                    if (now-sem_datetime).seconds>90: 
                        lock_ok=True
                    else:
                        reason_lock="["+`(now-sem_datetime).seconds`+" sec]"
                except:
                    lock_ok=True
                #print "lock status: "+str(lock_ok)
                if lock_ok:
                    pickle.dump(now,file("sem_"+project_name+".pkl", "w"))
                    self.log(project_name,id_instance+" - ("+project_name+" locked)")
                    self.queue=[]
                    try:
                        self.queue=pickle.load(file("queue_"+project_name+".pkl", "r"))
                    except: pass
                else:
                    self.log(project_name,id_instance+" locked "+reason_lock+". waiting.")
                    returnValue=",,"+"NOP"
            #lock_ok=True
            #encoded=""
            reset=False
            reason_reset=""
            if lock_ok:
                self.log(project_name,id_instance+" - step 1: cleaning")
                #cleaning
                isclean=False
                while not isclean:
                    idxClean=-1
                    ct=0
                    for q in self.queue:
                        self.log(project_name,q["instance"]+" not seen in the last "+`(now-q["timestamp"]).seconds`+" seconds")
                        if (now-q["timestamp"]).seconds>180:
                            idxClean=ct
                            if q["instance"]==id_instance:
                                reset=True
                                reason_reset="[cleaning]"
                            break
                        ct=ct+1
                    if idxClean!=-1:
                        self.log(project_name,"cleaning "+self.queue[idxClean]["instance"])
                        self.queue.remove(self.queue[idxClean])
                    else:
                        isclean=True

                if not reset:
                    newInstance=False
                    #common
                    idx=-1
                    ct=0
                    for q in self.queue:
                        if q["instance"]==id_instance:
                            idx=ct
                            break
                        ct=ct+1
                    if idx==-1:
                        q={}
                        q["instance"]=id_instance
                        self.queue.append(q)
                        idx=ct
                        newInstance=True
                    self.queue[idx]["state"]=state
                    self.queue[idx]["timestamp"]=now
                    self.queue[idx]["project_name"]=project_name
                    self.queue[idx]["username"]=username
                    self.queue[idx]["password"]=password

                    if self.queue[idx].has_key("partner"):
                        self.log(project_name,id_instance+" has partner. - step 1.9: partner validation")
                        found=False
                        for q in self.queue:
                            if q["instance"]==id_instance:
                                found=True
                                break
                        if not found: 
                            self.log(project_name,id_instance+" - partner "+self.queue[idx]["partner"]+" removed")
                            del self.queue[idx]["partner"]
                                    
                    #state machine
                    returnValue=",,NOP"
                    if newInstance and state!="A": 
                        reset=True
                        reason_reset="[new instance and state is not A]"
                    if not reset: 
                        self.log(project_name,id_instance+" - step 2: state machine begin")
                        idxPartner=-1
                        if state=="A":
                            if self.queue[idx].has_key("standby"): del self.queue[idx]["standby"]
                            data=zlib.decompress(base64.b64decode(encoded))
                            self.queue[idx]["md5Rows"]=data
                            #partner searching
                            ct=0
                            for q in self.queue:
                                self.log(project_name,q["instance"] + " " + `q.has_key("partner")`)
                                if not self.queue[idx].has_key("partner"):
                                    if not q.has_key("partner"):
                                        #if q["instance"]!=id_instance and q["md5Rows"]!=data and q["project_name"]==project_name:
                                        self.log(project_name,q["instance"]+"\t"+q["state"])
                                        if q["instance"]!=id_instance and q["md5Rows"]!=data and q["project_name"]==project_name and q["state"] in ["A","B","B1"]:
                                            idxPartner=ct
                                            self.queue[idxPartner]["partner"]=id_instance  #self.queue[idx]["instance"]
                                            #q["partner"]=id_instance  #self.queue[idx]["instance"]
                                            self.queue[idx]["partner"]=q["instance"]    #self.queue[idxPartner]["instance"]
                                            self.log(project_name,id_instance + " <-1-> " + q["instance"])
                                    elif q["partner"]==id_instance:
                                        idxPartner=ct
                                        self.queue[idx]["partner"]=q["instance"]
                                        self.log(project_name,id_instance + " <-2-> " + q["instance"])
                                #elif q["partner"]==id_instance:
                                else:
                                    if not q.has_key("partner"):
                                        pass
                                    elif q["partner"]==id_instance:
                                        idxPartner=ct
                                        self.log(project_name,id_instance + " <-3-> " + q["instance"])
                                ct=ct+1
                                # if idxPartner!=-1: break
                            self.log(project_name,id_instance + " - 2.01: end cycle")
                            if idxPartner!=-1:
                                encoded = base64.b64encode(zlib.compress(self.queue[idxPartner]["md5Rows"]))
                                returnValue=self.queue[idxPartner]["instance"]+","+self.queue[idxPartner]["state"]+","+encoded
                            else:
                                returnValue=",,NOP"
                        else:
                            idxPartner=-1
                            ct=0
                            for q in self.queue:
                                if q.has_key("partner"):
                                    if q["partner"]==id_instance:
                                        idxPartner=ct
                                        self.log(project_name,id_instance + " <-4-> " + q["instance"])
                                ct=ct+1
                                if idxPartner!=-1: break
                            if idxPartner==-1 and state!="E": 
                                reset=True
                                reason_reset="[partner disappeared]"

                        self.log(project_name,id_instance+" - step 2.1: ready for data input")
                        if self.queue[idx].has_key("standby"):
                            self.log(project_name,self.queue[idx]["instance"]+" has been idle for "+`int((now-self.queue[idx]["standby"]).seconds)`+" seconds.")
                            if (now-self.queue[idx]["standby"]).seconds>180: 
                                reset=True
                                reason_reset="[idle for >180 sec]"
                    #print str(reset)
                    self.log(project_name,id_instance+" - step 2.5: reset="+`reset`+" "+reason_reset)
                    if not reset: 
                        if idxPartner!=-1:
                            self.log(project_name,id_instance+" - step 3: data input")
                            if state=="B":
                                if self.queue[idx].has_key("standby"): del self.queue[idx]["standby"]
                                self.queue[idx]["lstRows"]=zlib.decompress(base64.b64decode(encoded))
                            elif state=="B1":
                                if not self.queue[idx].has_key("standby"): 
                                    self.queue[idx]["standby"]=now
                                    self.log(project_name,self.queue[idx]["instance"]+" from now in standby (B1)")
                            elif state=="C":
                                if self.queue[idx].has_key("standby"): del self.queue[idx]["standby"]
                                self.queue[idx]["deltaRows"]=zlib.decompress(base64.b64decode(encoded.split(",")[0]))
                                if len(encoded.split(","))>=2: self.queue[idx]["deltaContents_index"]=zlib.decompress(base64.b64decode(encoded.split(",")[1]))
                                else: self.queue[idx]["deltaContents_index"]=""
                                if len(encoded.split(","))>=3: self.queue[idx]["deltaHeaders"]=zlib.decompress(base64.b64decode(encoded.split(",")[2]))
                                else: self.queue[idx]["deltaHeaders"]=""
                            elif state=="C1":
                                if not self.queue[idx].has_key("standby"): 
                                    self.queue[idx]["standby"]=now
                                    self.log(project_name,self.queue[idx]["instance"]+" from now in standby (C1)")
                            elif state=="D":
                                if self.queue[idx].has_key("standby"): del self.queue[idx]["standby"]
                                self.queue[idx]["deltaDict"]=zlib.decompress(base64.b64decode(encoded))
                            elif state=="D1":
                                if not self.queue[idx].has_key("standby"): 
                                    self.queue[idx]["standby"]=now
                                    self.log(project_name,self.queue[idx]["instance"]+" from now in standby (D1)")


                            self.log(project_name,id_instance+" - step 4: data output")
                            # if self.queue[idxPartner]["state"] in ["A"]:
                            if state in ["A"]:
                                pass
                            # elif self.queue[idxPartner]["state"] in ["B","B1"]:
                            elif state in ["B","B1"] and self.queue[idxPartner]["state"] in ["B","B1","C","C1"]:
                                encoded="NOP"
                                if not self.queue[idx].has_key("sent_B"):
                                    self.queue[idx]["sent_B"]=True
                                    encoded = base64.b64encode(zlib.compress(self.queue[idxPartner]["lstRows"]))
                                    self.log(project_name,id_instance+" - step 4.5: sending md5Rows and lstRows")
                                    self.queue[idxPartner]["md5Rows"]=""
                                    self.queue[idxPartner]["lstRows"]=""
                                returnValue=self.queue[idxPartner]["instance"]+","+self.queue[idxPartner]["state"]+","+encoded
                            # elif self.queue[idxPartner]["state"] in ["C","C1"]:
                            elif state in ["C","C1"] and self.queue[idxPartner]["state"] in ["C","C1","D","D1"]:
                                encoded="NOP"
                                if not self.queue[idx].has_key("sent_C"):
                                    self.queue[idx]["sent_C"]=True
                                    encoded = base64.b64encode(zlib.compress(self.queue[idxPartner]["deltaRows"]))+","+base64.b64encode(zlib.compress(self.queue[idxPartner]["deltaContents_index"]))+","+base64.b64encode(zlib.compress(self.queue[idxPartner]["deltaHeaders"]))
                                    self.log(project_name,id_instance+" - step 4.5: sending deltaRows, deltaContents_index, deltaHeaders")
                                    self.queue[idxPartner]["deltaRows"]=""
                                    self.queue[idxPartner]["deltaContents_index"]=""
                                    self.queue[idxPartner]["deltaHeaders"]=""
                                    #in questo momento si e' perso il legame tra record (che vengono spediti ora) e dati (che devono ancora arrivare)
                                returnValue=self.queue[idxPartner]["instance"]+","+self.queue[idxPartner]["state"]+","+encoded
                            # elif self.queue[idxPartner]["state"] in ["D","D1"]:
                            elif state in ["D","D1"] and self.queue[idxPartner]["state"] in ["D","D1","E"]:
                                encoded="NOP"
                                if not self.queue[idx].has_key("sent_D"):
                                    self.queue[idx]["sent_D"]=True
                                    encoded = base64.b64encode(zlib.compress(self.queue[idxPartner]["deltaDict"]))
                                    self.log(project_name,id_instance+" - step 4.5: sending deltaDict")
                                    self.queue[idxPartner]["deltaDict"]=""
                                returnValue=self.queue[idxPartner]["instance"]+","+self.queue[idxPartner]["state"]+","+encoded
                            else:
                                returnValue=self.queue[idxPartner]["instance"]+","+self.queue[idxPartner]["state"]+","+"NOP"
                            self.log(project_name,id_instance+" - step 5: end data output")

                        if state=="E":
                            if self.queue[idx].has_key("standby"): del self.queue[idx]["standby"]
                            if idxPartner!=-1:
                                if self.queue[idxPartner]["state"] in ["E"]:
                                    self.log(project_name,id_instance+" has completed the syncronization, partner "+self.queue[idxPartner]["instance"]+" has state="+self.queue[idxPartner]["state"])
                                    self.queue.remove(self.queue[idx])
                                    # del self.queue[idxPartner]["partner"]
                                else:
                                    self.log(project_name,id_instance+" waiting the other end for completing the syncronization")
                            else:
                                self.log(project_name,id_instance+" has completed the syncronization, partner "+self.queue[idx]["partner"]+" has already completed.")
                                self.queue.remove(self.queue[idx])
                                returnValue=",,RES"
                    else:
                        self.queue.remove(self.queue[idx])
                        # self.queue.remove(self.queue[idxPartner])
                        returnValue=",,RES"
                        self.log(project_name,id_instance+" sending reset(1) because: "+reason_reset)

                else:
                    #qui reset e' True
                    returnValue=",,RES"
                    self.log(project_name,id_instance+" sending reset(2) because: "+reason_reset)
                    
                #salvataggio dati - inizio
                self.log(project_name,id_instance+" - step 6: pickle.dump")
                pickle.dump(self.queue,file("queue_"+project_name+".pkl", "w"))
                try:
                    import os
                    os.unlink("sem_"+project_name+".pkl")
                    self.log(project_name,id_instance+" - ("+project_name+" unlocked)")
                except: pass
                del self.queue[:]
                del self.queue
                #salvataggio dati - fine
            else:
                #qui lock e' False - devo aspettare senza cancellare il semaforo
                pass

            ##salvataggio dati - inizio
            #self.log(project_name,id_instance+" - step 6: pickle.dump")
            #pickle.dump(self.queue,file("queue_"+project_name+".pkl", "w"))
            #if lock_ok:
                #try:
                    #import os
                    #os.unlink("sem_"+project_name+".pkl")
                    #self.log(project_name,id_instance+" - ("+project_name+" unlocked)")
                #except: pass
            #del self.queue
            ##salvataggio dati - fine

            ct=0
            tolog=""
            for elm in returnValue.split(","):
                if ct>0: tolog=tolog+","
                if len(elm)<10: 
                    tolog=tolog+elm
                else:
                    tolog=tolog+"<encoded("+str(len(elm))+")>"
                ct=ct+1
            tolog=tolog+" - ["+str(len(returnValue.split(",")))+"]"
            self.log(project_name,"p2p - Output: "+tolog)
        except:
            self.log(project_name,id_instance+" ERROR:")
            import sys
            self.log(project_name,id_instance+" - "+`sys.exc_info()`)
            self.log(project_name,id_instance+" - "+`self.formatExceptionInfo()`)
        return returnValue

        
def main():
    import SimpleXMLRPCServer

    import socket
    socket.setdefaulttimeout(10)

    heavybaseservice_object = HeavyBaseService(True)
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(("", 60002))
    server.register_instance(heavybaseservice_object)

    #Go into the main listener loop
    print "Starting HeavyBaseService on port 60002"
    server.serve_forever()

if __name__ == '__main__':
    main()

