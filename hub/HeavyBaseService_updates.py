#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Name:        HeavyBaseService_updates.py
# Purpose:     Updater component for HeavyBase
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
# Copyright:   2006-2013 Luca Clivio
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

class HeavyBaseService_updates:
    def GetFilenames(self,project_name):
        if project_name.find("--")<0: project_name=project_name+"--"
        base_project_name=project_name.split("--")[0]
        base_version=project_name.split("--")[1]
        
        updates=""
        
        # NEGRI - TRIALS ONCOLOGIA
        if base_project_name in ("irfmn_alc", "irfmn_alc_demo"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_inovatyon", "irfmn_inovatyon_demo"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_ectict", "irfmn_ectict_demo"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_pact18","irfmn_pact18_test"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_atreus"):
            updates="heavybase.xrc|HeavyBase.py"
        elif base_project_name in ("irfmn_b490"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_b490_2"):
            updates="heavybase.xrc|HeavyBase.py"
        elif base_project_name in ("irfmn_glaucoma"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_terapora"):
            updates="heavybase.xrc|HeavyBase.py"
        elif base_project_name in ("irfmn_bevatrabe"):
            updates="heavybase.xrc|HeavyBase.py"
        elif base_project_name in ("irfmn_ortataxel"):
            updates="heavybase.xrc|HeavyBase.py"

        # NEGRI - TRIALS NEUROSCIENZE
        if base_project_name in ("irfmn_RF20091502045_test"):
            updates="heavybase.xrc|HeavyBase.py"
        if base_project_name in ("irfmn_prolong"):
            updates="heavybase.xrc|HeavyBase.py"
        
        # NEGRI - REGISTRI
        if base_project_name in ("irfmn_eurals","irfmn_eurals_01"):
            updates="HeavyBase.py"
        elif base_project_name in ("irfmn_anaconda","irfmn_anaconda_test"):
            updates="icd9cm_24.xls|heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("irfmn_anaconda_2"):
            updates="heavybase.xrc|HeavyBase.py"
            
        # NEGRI - BIOBANCHE
        if base_project_name=="erba_linee_cellulari":
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name=="igc3_monza_v3":
            updates="heavybase.xrc|template.abw|update.py|HeavyBase.py"
        elif base_project_name=="igc3_monza_v4":
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name=="irfmn_xenohoc":
            updates="heavybase.xrc|template.rtf|HeavyBase.py"

        # IEO
        #giscor rectum
        if base_project_name=="giscor_rectum":
            updates="heavybase.xrc|quickreport.py|update.py|HeavyBase.py"
        #sound
        if base_project_name in ("ieo_s637_311","ieo_s637_311_r1"):
            updates="update.py|heavybase.xrc|analysis.py|quickreport.py|HeavyBase.py"
        #resezioni limitate
        if base_project_name=="ieo_s638_311":
            updates="heavybase.xrc|analysis.py|quickreport.py|HeavyBase.py"
        #cosmos II
        if base_project_name=="ieo_s669_511":
            updates="update.py|heavybase.xrc|analysis.py|quickreport.py|template_tac_2_anno.rtf|HeavyBase.py"
        #mirasole
        if base_project_name=="ieo_s639_311_03":
            updates="heavybase.xrc|HeavyBase.py"
        #nam-breast
        if base_project_name=="ieo_nam-breast":
            updates="template_consulti_adiuvante_1.rtf|template_consulti_metastatico_1.rtf|template_consulti_metastatico_3.rtf|template_consulti_adiuvante_2.rtf|template_consulti_metastatico_2.rtf|template_consulti_preoperatoria.rtf|update.py|p2pdb.xrc|P2Pdb.py"
        
        # INT
        #resort
        if base_project_name=="int_resort":
            updates="P2Pdb.py"
        #post-alk
        if base_project_name=="int_post-alk":
            updates="P2Pdb.py"
        #lume
        if base_project_name=="int_lume":
            updates="update.py|p2pdb.xrc|P2Pdb.py"
        
        # POLICLINICO
        #ralp01,2,3
        if base_project_name in ("ralp01","ralp02","ralp03"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
            
        # BELLINZONA
        #ielsg37_01
        if base_project_name=="ielsg37_01":
            updates="HeavyBase.py"
            
        # ROL
        if base_project_name=="rol_pdta-bda":
            updates="heavybase.xrc|update.py|HeavyBase.py"

        # CHI2
        if base_project_name=="chi2_pact15":
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name in ("chi2_victor2","chi2_victor2_02","chi2_victor2_test"):
            updates="heavybase.xrc|update.py|HeavyBase.py"
        if base_project_name=="chi2_initio_02":
            updates="heavybase.xrc|HeavyBase.py"

        # SVHR
        if base_project_name=="highresearch_maffeis":
            updates="heavybase.bat|heavybase_windows.bat|HeavyBase_library1.zip|HeavyBase_library2.zip|heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name=="highresearch_whee":
            updates="HeavyBase_library1.zip|HeavyBase_library2.zip|heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name=="svhr_nemo_02":
            updates="heavybase.xrc|update.py|HeavyBase.py"
        elif base_project_name=="svhr_venere_01_test":
            updates="heavybase.xrc|update.py|HeavyBase.py"

        # BG
        if base_project_name=="bg_asaa":
            updates="heavybase.xrc|update.py|HeavyBase.py"
            
        # MONTICELLO
        #if base_project_name=="mont_adi_03":
        #    updates="heavybase.xrc|analysis.py|update.py|HeavyBase.py"
        if base_project_name=="mont_adi_04":
            updates=""
            if base_version < "4.7.5":
                updates="HeavyBase_library5.zip"
            if updates!="": updates=updates+"|"
            updates=updates+"prestazioni.xls|profili_di_cura.xls|heavybase.xrc|analysis.py|HeavyBase.py"
            
        # IMPAPERLESS
        if base_project_name in ("impaperless_02","impaperless_03"):
            updates=""
            if base_version < "4.9.7":
                updates="template_anagrafica.rtf|template_grafoterapista.rtf|template_neuropsichiatra.rtf|template_psicomotricista.rtf|template_dsa.rtf|template_logopedista.rtf|template_pedagogista.rtf|template_psicoterapeuta.rtf|template_feuerstein.rtf|template_mediatrice.rtf|template_psicologo.rtf"
            elif base_version < "5.0.0":
                updates="template_anagrafica.rtf|template_neuropsichiatra.rtf"
            if updates!="": updates=updates+"|"
            updates=updates+"heavybase.xrc|update.py|HeavyBase.py"
            
        # FAILBACK
        if updates=="" and base_project_name!="sinpe_domus":
            updates="HeavyBase.py"
           
        # HeavyBase_library4 parametrico
        if base_version < "4.6.5":
            if updates!="": updates="HeavyBase_library4.zip|"+updates
            
        # RESTART PARAMETRICO
        if base_version < "4.5.9":
            if updates!="": updates=updates+"|"
            updates=updates+"restart.cmd"
        
        return updates

