# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot


snapshots = Snapshot()

snapshots["test_electric_benefits_full_outputs elec_ben"] = GenericRepr(
    "                             identifier  ...  methane_leakage\n0                                     0  ...      2440.456961\n1                                     0  ...      2614.282898\n2                                     0  ...      2114.712496\n3                                     0  ...      1617.374298\n4                                     0  ...      2150.869447\n...                                 ...  ...              ...\n23323  Res_RefgFrzr_Recyc_UnConditioned  ...        10.331944\n23324  Res_RefgFrzr_Recyc_UnConditioned  ...        10.269918\n23325  Res_RefgFrzr_Recyc_UnConditioned  ...         8.232520\n23326  Res_RefgFrzr_Recyc_UnConditioned  ...         6.383223\n23327  Res_RefgFrzr_Recyc_UnConditioned  ...         5.616305\n\n[23328 rows x 17 columns]"
)

snapshots["test_user_inputs_basic df_output_table_totals"] = GenericRepr(
    "TRC                                         2.922671e+03\nPAC                                         5.186274e+03\nTRC (and PAC) Electric Benefits ($)         6.861880e+08\nTRC (and PAC) Gas Benefits ($)              7.766636e+04\nTRC (and PAC) Total Benefits ($)            6.862657e+08\nTRC Costs ($)                               2.348077e+05\nPAC Costs ($)                               1.323234e+05\nElectricity First Year Net Savings (MWh)    9.086439e+05\nElectricity Lifecycle Net Savings (MWh)     8.177795e+06\nGas First Year Net Savings (Therms)         9.329000e+03\nGas Lifecycle Net Savings (Therms)          8.396100e+04\nElectricity Lifecycle GHG Savings (Tons)    2.301818e+06\nGas Lifecycle GHG Savings (Tons)            5.037660e+02\nTotal Lifecycle GHG Savings (Tons)          2.302322e+06\ndtype: float64"
)

snapshots["test_user_inputs_from_example_metered df_output_table_totals"] = GenericRepr(
    "TRC                                            2.424\nPAC                                            4.623\nTRC (and PAC) Electric Benefits ($)         -626.240\nTRC (and PAC) Gas Benefits ($)              5624.770\nTRC (and PAC) Total Benefits ($)            4998.530\nTRC Costs ($)                               2062.420\nPAC Costs ($)                               1081.210\nElectricity First Year Net Savings (MWh)      -0.932\nElectricity Lifecycle Net Savings (MWh)      -13.984\nGas First Year Net Savings (Therms)          400.000\nGas Lifecycle Net Savings (Therms)          6000.000\nElectricity Lifecycle GHG Savings (Tons)      -4.936\nGas Lifecycle GHG Savings (Tons)              36.000\nTotal Lifecycle GHG Savings (Tons)            31.064\ndtype: float64"
)

snapshots["test_user_inputs_full df_output_table_totals"] = GenericRepr(
    "TRC                                         2.922671e+03\nPAC                                         5.186274e+03\nTRC (and PAC) Electric Benefits ($)         6.861880e+08\nTRC (and PAC) Gas Benefits ($)              7.766636e+04\nTRC (and PAC) Total Benefits ($)            6.862657e+08\nTRC Costs ($)                               2.348077e+05\nPAC Costs ($)                               1.323234e+05\nElectricity First Year Net Savings (MWh)    9.086439e+05\nElectricity Lifecycle Net Savings (MWh)     8.177795e+06\nGas First Year Net Savings (Therms)         9.329000e+03\nGas Lifecycle Net Savings (Therms)          8.396100e+04\nElectricity Lifecycle GHG Savings (Tons)    2.301818e+06\nGas Lifecycle GHG Savings (Tons)            5.037660e+02\nTotal Lifecycle GHG Savings (Tons)          2.302322e+06\ndtype: float64"
)

snapshots["test_user_inputs_single_row df_output_table_totals"] = GenericRepr(
    "TRC                                             25.606\nPAC                                             27.760\nTRC (and PAC) Electric Benefits ($)         332420.750\nTRC (and PAC) Gas Benefits ($)                7834.680\nTRC (and PAC) Total Benefits ($)            340255.430\nTRC Costs ($)                                13287.870\nPAC Costs ($)                                12256.910\nElectricity First Year Net Savings (MWh)       419.101\nElectricity Lifecycle Net Savings (MWh)       3771.906\nGas First Year Net Savings (Therms)            950.000\nGas Lifecycle Net Savings (Therms)            8550.000\nElectricity Lifecycle GHG Savings (Tons)      1060.144\nGas Lifecycle GHG Savings (Tons)                51.300\nTotal Lifecycle GHG Savings (Tons)            1111.444\ndtype: float64"
)
