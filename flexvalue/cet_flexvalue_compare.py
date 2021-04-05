import os
import shutil
from zipfile import ZipFile
import glob
import re
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory

programcost_columns = "PrgID|PrgYear|ClaimYearQuarter|AdminCostsOverheadAndGA|AdminCostsOther|MarketingOutreach|DIActivity|DIInstallation|DIHardwareAndMaterials|DIRebateAndInspection|EMV|UserInputIncentive|OnBillFinancing|CostsRecoveredFromOtherSources|PA"

measure_columns = "CEInputID|PrgID|ClaimYearQuarter|Sector|DeliveryType|BldgType|E3ClimateZone|E3GasSavProfile|E3GasSector|E3MeaElecEndUseShape|E3TargetSector|MeasAppType|MeasCode|MeasDescription|MeasImpactType|MeasureID|TechGroup|TechType|UseCategory|UseSubCategory|PreDesc|StdDesc|SourceDesc|Version|NormUnit|NumUnits|UnitkW1stBaseline|UnitkWh1stBaseline|UnitTherm1stBaseline|UnitkW2ndBaseline|UnitkWh2ndBaseline|UnitTherm2ndBaseline|UnitMeaCost1stBaseline|UnitMeaCost2ndBaseline|UnitDirectInstallLab|UnitDirectInstallMat|UnitEndUserRebate|UnitIncentiveToOthers|NTG_ID|NTGRkW|NTGRkWh|NTGRTherm|NTGRCost|EUL_ID|EUL_Yrs|RUL_ID|RUL_Yrs|GSIA_ID|RealizationRatekW|RealizationRatekWh|RealizationRateTherm|InstallationRatekW|InstallationRatekWh|InstallationRateTherm|Residential_Flag|Upstream_Flag|PA|MarketEffectsBenefits|MarketEffectsCosts|RateScheduleElec|RateScheduleGas|CombustionType|MeasInflation|Comments"

DEER_NonRes = [
    "DEER:HVAC_Chillers",
    "DEER:HVAC_Split-Package_AC",
    "DEER:HVAC_Split-Package_HP",
    "DEER:Indoor_Non-CFL_Ltg",
]

discount_rate = 0.0766


def generate_cet_input_id(program_admin, program_year, identifier):
    return f"{program_admin}-{program_year}-{identifier}"


class CET_Scan:
    def __init__(
        self,
        program_year,
        acc_version,
        program_admin,
        climate_zone,
        mwh,
        therms,
        units,
        ntg,
        eul,
        sector,
        deer_load_shape,
        gas_sector,
        gas_savings_profile,
        admin_cost,
        measure_cost,
        incentive,
        directory=".",
        scan_name="Test",
    ):
        self.directory = directory
        self.scan_name = scan_name
        self.path = os.path.join(directory, scan_name)

        self.cet_path = os.path.join(self.path, "cet")
        Path(self.cet_path).mkdir(parents=True, exist_ok=True)
        self.cet_zip_path = os.path.join(self.cet_path, f"{self.scan_name}.zip")

        self.flexvalue_path = os.path.join(self.path, "flexvalue")
        Path(self.flexvalue_path).mkdir(parents=True, exist_ok=True)

        self.program_year = program_year
        self.acc_version = acc_version
        self.program_admin = program_admin
        self.climate_zone = climate_zone
        self.units = units
        self.ntg = ntg
        self.eul = eul
        self.sector = sector
        self.deer_load_shape = deer_load_shape
        self.gas_sector = gas_sector
        self.gas_savings_profile = gas_savings_profile
        self.admin_cost = admin_cost
        self.measure_cost = measure_cost
        self.incentive = incentive

        self.index = [110] + list(111 + i for i in range(len(mwh) - 1))
        self.kwh = [m * u * 1000 for m, u in zip(list(i for i in mwh), units)]
        self.mwh = [m * u for m, u in zip(list(i for i in mwh), units)]
        self.therms = [t * u for t, u in zip(list(i for i in therms), units)]

    def generate_cet_input_file(self):

        # Create Folders and Path
        Path(self.path).mkdir(parents=True, exist_ok=True)

        # Create ProgramCost.csv file for CET and write columns
        fname_costs = "ProgramCost.csv"

        # Create Measure.csv file for CET and write columns
        fname_measure = "Measure.csv"

        # Add lines to CET ProgramCost and Measure files, scanning over variable

        def _generate_program_id(program_admin, identifier):
            return f"{program_admin}-{identifier}"

        def _generate_claim_year_quarter(program_year):
            return f"{program_year}Q1"

        def _generate_e3_target_sector(deer_load_shape, sector):
            return "Non_Res" if deer_load_shape in DEER_NonRes else sector

        for ind in range(len(self.mwh)):
            if self.sector[ind] == "Res" and self.deer_load_shape[ind] in DEER_NonRes:
                print(
                    f"{self.sector[ind]}/{self.deer_load_shape[ind]}"
                    + " Pairing Not Allowed in CET. Switching to Non_Res"
                )

        cet_program_costs_df = pd.DataFrame(
            [
                {
                    "PrgID": _generate_program_id(self.program_admin, self.index[ind]),
                    "PrgYear": self.program_year,
                    "ClaimYearQuarter": f"{self.program_year}Q1",
                    "AdminCostsOverheadAndGA": self.admin_cost[ind],
                    "AdminCostsOther": 0,
                    "MarketingOutreach": 0,
                    "DIActivity": 0,
                    "DIInstallation": 0,
                    "DIHardwareAndMaterials": 0,
                    "DIRebateAndInspection": 0,
                    "EMV": 0,
                    "UserInputIncentive": 0,
                    "OnBillFinancing": 0,
                    "CostsRecoveredFromOtherSources": 0,
                    "PA": self.program_admin,
                }
                for ind in range(len(self.mwh))
            ]
        )

        cet_measure_costs_df = pd.DataFrame(
            [
                {
                    "CEInputID": generate_cet_input_id(
                        self.program_admin, self.program_year, self.index[ind]
                    ),
                    "PrgID": _generate_program_id(self.program_admin, self.index[ind]),
                    "ClaimYearQuarter": f"{self.program_year}Q1",
                    "Sector": "Commercial",
                    "DeliveryType": "CustIncentDown",
                    "BldgType": "Com",
                    "E3ClimateZone": self.climate_zone[ind],
                    "E3GasSavProfile": self.gas_savings_profile[ind],
                    "E3GasSector": self.gas_sector[ind],
                    "E3MeaElecEndUseShape": self.deer_load_shape[ind],
                    "E3TargetSector": _generate_e3_target_sector(
                        self.deer_load_shape[ind], self.sector[ind]
                    ),
                    "MeasAppType": "AR",
                    "MeasCode": "",
                    "MeasDescription": "NMEC",
                    "MeasImpactType": "Cust-NMEC",
                    "MeasureID": "0",
                    "TechGroup": "",
                    "TechType": "Pilot",
                    "UseCategory": "",
                    "UseSubCategory": "Testing",
                    "PreDesc": "",
                    "StdDesc": "",
                    "SourceDesc": "",
                    "Version": "",
                    "NormUnit": "Each",
                    "NumUnits": 1,
                    "UnitkW1stBaseline": 0,
                    "UnitkWh1stBaseline": self.kwh[ind],
                    "UnitTherm1stBaseline": self.therms[ind],
                    "UnitkW2ndBaseline": 0,
                    "UnitkWh2ndBaseline": 0,
                    "UnitTherm2ndBaseline": 0,
                    "UnitMeaCost1stBaseline": self.measure_cost[ind],
                    "UnitMeaCost2ndBaseline": 0,
                    "UnitDirectInstallLab": 0,
                    "UnitDirectInstallMat": 0,
                    "UnitEndUserRebate": self.incentive[ind],
                    "UnitIncentiveToOthers": 0,
                    "NTG_ID": "NonRes-sAll-NMEC",
                    "NTGRkW": self.ntg[ind],
                    "NTGRkWh": self.ntg[ind],
                    "NTGRTherm": self.ntg[ind],
                    "NTGRCost": self.ntg[ind],
                    "EUL_ID": "",
                    "EUL_Yrs": self.eul[ind],
                    "RUL_ID": "",
                    "RUL_Yrs": 0,
                    "GSIA_ID": "",
                    "RealizationRatekW": 1,
                    "RealizationRatekWh": 1,
                    "RealizationRateTherm": 1,
                    "InstallationRatekW": 1,
                    "InstallationRatekWh": 1,
                    "InstallationRateTherm": 1,
                    "Residential_Flag": 0,
                    "Upstream_Flag": 0,
                    "PA": self.program_admin,
                    "MarketEffectsBenefits": "",
                    "MarketEffectsCosts": "",
                    "RateScheduleElec": "",
                    "RateScheduleGas": "",
                    "CombustionType": "",
                    "MeasInflation": "",
                    "Comments": "",
                }
                for ind in range(len(self.mwh))
            ]
        )

        with TemporaryDirectory() as tmpdirname:
            program_cost_filepath = os.path.join(tmpdirname, "ProgramCost.csv")
            cet_program_costs_df.to_csv(program_cost_filepath, index=False, sep="|")

            measure_filepath = os.path.join(tmpdirname, "Measure.csv")
            cet_measure_costs_df.to_csv(measure_filepath, index=False, sep="|")

            zip_filepath = os.path.join(tmpdirname, "zip_file.zip")
            with ZipFile(zip_filepath, "w") as zip_obj:
                zip_obj.write(measure_filepath, arcname="Measure.csv")
                zip_obj.write(program_cost_filepath, arcname="ProgramCost.csv")
            shutil.move(zip_filepath, self.cet_zip_path)

        print(f"Your CET input file is at {self.cet_zip_path}")

        def _get_flexvalue_load_shape_name(deer_load_shape, sector):
            load_shape_suffix = deer_load_shape[5:].upper().replace("-", "_")
            load_shape_prefix = (
                "NONRES" if deer_load_shape in DEER_NonRes else sector.upper()
            )
            return f"{load_shape_prefix}_{load_shape_suffix}"

        user_inputs = pd.DataFrame(
            [
                {
                    "ID": self.index[ind],
                    "start_year": self.program_year,
                    "start_quarter": 1,
                    "utility": self.program_admin,
                    "climate_zone": self.climate_zone[ind],
                    "mwh_savings": self.mwh[ind],
                    "load_shape": _get_flexvalue_load_shape_name(
                        self.deer_load_shape[ind], self.sector[ind]
                    ),
                    "therms_savings": self.therms[ind],
                    "therms_profile": self.gas_savings_profile[ind].split(" ")[0],
                    "units": self.units[ind] / self.units[ind],
                    "eul": self.eul[ind],
                    "ntg": self.ntg[ind],
                    "discount_rate": discount_rate,
                    "admin": self.admin_cost[ind],
                    "measure": self.measure_cost[ind],
                    "incentive": self.incentive[ind],
                }
                for ind in range(len(self.mwh))
            ]
        )

        user_inputs_filepath = os.path.join(
            self.flexvalue_path, f"{self.scan_name}_flexvalue_user_inputs.csv"
        )
        user_inputs.to_csv(user_inputs_filepath)

        print(f"Your FLEXvalue input file is at {user_inputs_filepath}")
        return user_inputs.set_index('ID')

    def generate_cet_input_file_orig(self, files="both"):

        # Create Folders and Path
        Path(self.path).mkdir(parents=True, exist_ok=True)

        if files == "both" or files == "cet_only":
            # Create ProgramCost.csv file for CET and write columns
            fname_costs = "ProgramCost.csv"
            fhand_costs = open(fname_costs, "w")
            print(programcost_columns, file=fhand_costs)

            # Create Measure.csv file for CET and write columns
            fname_measure = "Measure.csv"
            fhand_measure = open(fname_measure, "w")
            print(measure_columns, file=fhand_measure)

            # Add lines to CET ProgramCost and Measure files, scanning over variable

            for ind in range(len(self.mwh)):

                print(
                    "%s0%g|%s|%sQ1|%g|0|0|0|0|0|0|0|0|0|0|%s"
                    % (
                        self.program_admin,
                        self.index[ind],
                        self.program_year,
                        self.program_year,
                        self.admin_cost[ind],
                        self.program_admin,
                    ),
                    file=fhand_costs,
                )

                print(
                    "%s-%s-0%g|%s0%g|%sQ1|Commercial|CustIncentDown|Com|%s|%s|%s|%s|%s|AR||NMEC|Cust-NMEC|0||Pilot||Testing|||||Each|1|0|%g|%g|0|0|0|%g|0|0|0|%g|0|NonRes-sAll-NMEC|%g|%g|%g|%g||%g||0||1|1|1|1|1|1|0|0|%s|||||||"
                    % (
                        self.program_admin,
                        self.program_year,
                        self.index[ind],
                        self.program_admin,
                        self.index[ind],
                        self.program_year,
                        self.climate_zone[ind],
                        self.gas_savings_profile[ind],
                        self.gas_sector[ind],
                        self.deer_load_shape[ind],
                        "Non_Res"
                        if self.deer_load_shape[ind] in DEER_NonRes
                        else self.sector[ind],
                        self.kwh[ind],
                        self.therms[ind],
                        self.measure_cost[ind],
                        self.incentive[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.ntg[ind],
                        self.eul[ind],
                        self.program_admin,
                    ),
                    file=fhand_measure,
                )

                if (
                    self.sector[ind] == "Res"
                    and self.deer_load_shape[ind] in DEER_NonRes
                ):
                    print(
                        self.sector[ind]
                        + "/"
                        + self.deer_load_shape[ind]
                        + " Pairing Not Allowed in CET. Switching to Non_Res"
                    )

            # Close ProgramCost and Measure files
            fhand_costs.close()
            fhand_measure.close()

            zipObj = ZipFile(self.cet_zip_path, "w")
            zipObj.write(fname_measure)
            zipObj.write(fname_costs)
            zipObj.close()

            print(f"Your CET input file is at {self.cet_zip_path}")

        if files == "both" or files == "flexvalue_only":

            user_inputs = pd.DataFrame(
                [
                    {
                        "ID": self.index[ind],
                        "start_year": self.program_year,
                        "start_quarter": 1,
                        "utility": self.program_admin,
                        "climate_zone": self.climate_zone[ind],
                        "mwh_savings": self.mwh[ind],
                        "load_shape": "NONRES_"
                        + self.deer_load_shape[ind][5:].upper().replace("-", "_")
                        if self.deer_load_shape[ind] in DEER_NonRes
                        else self.sector[ind].upper()
                        + "_"
                        + self.deer_load_shape[ind][5:].upper().replace("-", "_"),
                        "therms_savings": self.therms[ind],
                        "therms_profile": self.gas_savings_profile[ind].split(" ")[0],
                        "units": self.units[ind] / self.units[ind],
                        "eul": self.eul[ind],
                        "ntg": self.ntg[ind],
                        "discount_rate": discount_rate,
                        "admin": self.admin_cost[ind],
                        "measure": self.measure_cost[ind],
                        "incentive": self.incentive[ind],
                    }
                    for ind in range(len(self.mwh))
                ]
            )

            user_inputs_filepath = os.path.join(
                self.flexvalue_path, f"{self.scan_name}_flexvalue_user_inputs.csv"
            )
            user_inputs.to_csv(user_inputs_filepath)

            print(f"Your FLEXvalue input file is at {user_inputs_filepath}")
            return user_inputs

    def parse_cet_output(self, cet_output_filepath=None):

        # Create file to store key results

        glob_search_str = os.path.join(self.cet_path, self.scan_name + "_*.zip")
        loc_search = glob.glob(glob_search_str)
        if loc_search:
            loc = loc_search[0]
        else:
            raise ValueError(f"Can not find CET output zip file in {glob_search_str}")
        fname = os.path.basename(loc)

        # Unzip output files
        with ZipFile(self.path + "/cet/" + fname, "r") as zip_ref:
            zip_ref.extractall(self.path + "/cet/")

        # Extract and print key results to file
        fnum = re.findall(".*cet_ui_run_([0-9]+)", fname)[0]
        output_filepath = os.path.join(self.cet_path, f"{fnum}_outputs.csv")
        return pd.read_csv(output_filepath, delimiter="|")

    def compare_cet_to_flexvalue(self, cet_output_df, flexvalue_output_df):
        flexvalue_output_df["CET_ID"] = flexvalue_output_df.apply(
            lambda x: generate_cet_input_id(x["utility"], x["start_year"], x["ID"]),
            axis=1,
        )
        flexvalue_output_df["source"] = "flexvalue"
        flexvalue_output_df = flexvalue_output_df.reset_index().set_index(
            ["CET_ID", "source"]
        )

        cet_output_df = cet_output_df.set_index("CET_ID").rename(
            columns={
                "ElecBen": "TRC (and PAC) Electric Benefits ($)",
                "GasBen": "TRC (and PAC) Gas Benefits ($)",
                "TRCCost": "TRC Costs ($)",
                "PACCost": "PAC Costs ($)",
                "TRCRatio": "TRC",
                "PACRatio": "PAC",
            }
        )
        cet_output_df["source"] = "CET"

        compare_cols = [
            "TRC (and PAC) Electric Benefits ($)",
            "TRC (and PAC) Gas Benefits ($)",
            "TRC Costs ($)",
            "PAC Costs ($)",
            "TRC",
            "PAC",
        ]
        cet_output_df = cet_output_df.reset_index().set_index(["CET_ID", "source"])
        return (
            pd.concat([flexvalue_output_df[compare_cols], cet_output_df[compare_cols]])
            .sort_index()
            .round(2)
        )
