import sqlite3

import pandas as pd


class ParamGenerator:
    _default_params = {
        "aging": "1/(1 + (x/0.50)^2.50)",
        "finerootFoliageRatio": 0.75,
        "sapReferenceRatio": 0.8,
        "browsingprobability": 0.2,
        "estSprouting": 0,
        "sapSproutGrowth": 0,
    }

    # see http://iland.boku.ac.at/species+parameter for lots and lots of details
    _schema = {
        "turnoverLeaf": ("leaf turnover", "turnover"),
        "turnoverRoot": ("root turnover", "turnover"),
        "HDlow": ("HDlow", "static_formula"),
        "HDhigh": ("HDhi", "static_formula"),
        "woodDensity": ("woodDensity", "density"),
        "formFactor": ("formFactor", "formFactor"),
        "bmWoody_a": ("bmWoody", "a"),
        "bmWoody_b": ("bmWoody", "b"),
        "bmRoot_a": ("bmRoot", "a"),
        "bmRoot_b": ("bmRoot", "b"),
        "bmBranch_a": ("bmBranch", "a"),
        "bmBranch_b": ("bmBranch", "b"),
        "bmFoliage_a": ("bmFoliage", "a"),
        "bmFoliage_b": ("bmFoliage", "b"),
        "probIntrinsic": ("mortality", "probIntrinsic"),
        "probStress": ("mortality", "probStress"),
        "maximumAge": ("maximumAge", "age"),
        "maximumHeight": ("maximumHeight", "maximumHeight"),
        "aging": None,
        "respVpdExponent": ("respVpdExponent", "Exponent",),
        "respTempMin": ("respTemp", "min"),
        "respTempMax": ("respTemp", "max"),
        "respNitrogenClass": ("respNitrogenClass", "Nclass"),
        "phenologyClass": ("phenologyClass", "phenologyClass"),
        "maxCanopyConductance": ("maxCanopyConductance", "vmax"),
        "psiMin": ("psiMin", "psiMin"),
        "lightResponseClass": (
            "lightResponseClass",
            "shade_tolerance",
        ),  # double check with WH
        "finerootFoliageRatio": None,
        "maturityYears": ("maturity", "maturity"),
        "seedYearInterval": ("seedyears", "average seed year interval"),
        "nonSeedYearFraction": ("seedyears", "non-seedyears"),
        "fecundity_m2": ("fecundity", "seeds per m^2 canopy"),
        "seedKernel_as1": ("dispersal", "kernel_as1"),
        "seedKernel_as2": ("dispersal", "kernel_as2"),
        "seedKernel_ks0": ("dispersal", "kernel_ks0"),
        "estMinTemp": ("establishment_iLand", "minTemp"),
        "estChillRequirement": ("establishment_iLand", "ChillRequirement"),
        "estGDDMin": ("establishment_iLand", "GDDmin"),
        "estGDDMax": ("establishment_iLand", "GDDmax"),
        "estGDDBaseTemp": ("establishment_iLand", "GDDbase"),
        "estBudBirstGDD": ("establishment_iLand", "BudBurstGDD"),
        "estFrostFreeDays": ("establishment_iLand", "FrostFreeDays"),
        "estFrostTolerance": ("establishment_iLand", "FrostTolerance"),
        "sapHeightGrowthPotential": (
            "sapling_growth",
            "static_formula",
        ),  # TODO: double check formula. WH version has \n in PIMA formula
        "sapMaxStressYears": ("sapMaxStressYear", "sapMaxStressYear"),
        "sapStressThreshold": ("sapStressThreshold", "sapStressThreshold"),
        "sapHDSapling": ("SDI", "hd FIA"),  # @WH
        "sapReinekesR": ("SDI", "Reineke new"),
        "sapReferenceRatio": None,  # TODO: WH going to consider refinements
        "cnFoliage": ("CN-ratios", "cnFoliage"),
        "cnFineroot": ("CN-ratios", "cnFineroot"),
        "cnWood": ("CN-ratios", "cnWood"),
        "snagKSW": ("decomp", "ksw"),
        "snagHalfLife": ("snags", "halflife"),
        "snagKYL": ("decomp", "kyl"),
        "snagKYR": ("decomp", "kyr",),
        "barkThickness": ("barkThickness", "Bark Thickness"),
        "browsingProbability": None,
        "estPsiMin": ("estPsiMin", "psiMin",),
        "estSprouting": None,  # TODO: species specific work needed. @john/matt
        "sapSproutGrowth": None,
        "serotinyFormula": None,  # TODO: some of the oaks might hold acorns over winter? Ask John.
        "serotinyFecundity": None,
    }

    _EXCEL_PARAM_FNAME = "~/mnt/data/iland_america.xlsx"
    _sheets = {}  # for caching sheets w multiple params.

    def __init__(self, short_name):
        self._short_name = short_name

    def _get_param(self, param_key):
        try:
            sheet_name, col_name = self._schema[param_key]
        except TypeError:
            # Param takes value None, get from defaults
            if self._schema[param_key] is None:
                return self._default_params.get(param_key, -999)
            else:
                raise
        try:
            sh = self._sheets[sheet_name]
        except KeyError:
            # naively cache -- yay!
            sh = pd.read_excel(self._EXCEL_PARAM_FNAME, sheet_name=sheet_name)
            self._sheets[sheet_name] = sh

        return sh.loc[sh.shortName == self._short_name].iloc[0][col_name]

    def _species_overrides(self):
        """
        After loading from the DB, search for species specific yaml/json(?) files to overwrite
        Like isConiferous, isEvergreen, active, displayColor, LIPFile
        estSprouting: 0/1
        """
        return {
            "shortName": self._short_name,
            "active": 1,
            "isEvergreen": 0,
            "isConiferous": 0,
        }

    def get_params(self):
        # TODO: check for -999 fill values.
        params = {k: self._get_param(k) for k in self._schema.keys()}
        overrides = self._species_overrides()
        params.update(overrides)
        return params


def main():
    out_sql_fname = "/tmp/species_params.sql"

    params = []

    brf_short_names = ["qumo", "quru", "bepa", "bele", "acpe", "acru"]
    for brf_short_name in brf_short_names:
        param_generator = ParamGenerator(brf_short_name)
        params.append(param_generator.get_params())

    species = pd.DataFrame(params)
    species.to_csv("/tmp/test.csv")

    con = sqlite3.connect(out_sql_fname)
    species.to_sql("species", con, if_exists="replace")


if __name__ == "__main__":
    main()
