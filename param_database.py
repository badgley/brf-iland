import pandas as pd

# {'sqlite_name': {'sheet_name': 'columname'}}

# http://iland.boku.ac.at/species+parameter
class ParameterStore:
    _schema = {
        "turnoverLeaf": ("leaf turnover", "turnover"),
        "turnoverRoot": ("root turnover", "turnover"),
        # "HDlow": ("HDlow", "derived"),  # TK need talk to winslow
        # "HDhigh": ("HDhi", "derived"),  # not clear how to do...
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
        # "aging": ("TK", "TK"),
        "respVpdExponent": (
            "respVpdExponent",
            "Exponent",
        ),  # Ask WH -- his potr number super different
        "respTempMin": ("respTemp", "min"),
        "respTempMax": ("respTemp", "max"),
        "respNitrogenClass": ("respNitrogenClass", "Nclass"),
        "phenologyClass": ("phenologyClass", "phenologyClass"),
        "maxCanopyConductance": ("maxCanopyConductance", "vmax"),
        "psiMin": ("psiMin", "psiMin"),
        "lightResponseClass": (
            "lightResponseClass",
            "Shade tolerance",
        ),  # double check with winslow
        # "finerootFoliageRatio": (TK, TK), #@WH
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
        # "sapHeightGrowthPotential": (TK, TK), #@WH
        "sapMaxStressYears": ("sapMaxStressYear", "sapMaxStressYear"),
        "sapStressThreshold": ("sapStressThreshold", "sapStressThreshold"),
        # "sapHDSapling": (TK, TK), # @WH
        # "sapReinekesR": (TK, TK), # @WH
        # "sapReferenceRatio": (TK, TK), # @WH
        "cnFoliage": ("CN-ratios", "cnFoliage"),
        "cnFineroot": ("CN-ratios", "cnFineroot"),
        "cnWood": ("CN-ratios", "cnWood"),
        "snagKSW": ("decomp", "ksw"),
        "snagHalfLife": ("snags", "halflife"),
        "snagKYL": ("decomp", "kyl"),
        "snagKYR": (
            "decomp",
            "kyr",
        ),  # double check WH that all are from `decomp` table
        "barkThickness": ("barkThickness", "Bark Thickness"),
        # "browsingProbability": (TK, TK),
        "estPsiMin": (
            "estPsiMin",
            "psiMin",
        ),  # how is this different from other Psi min?
        # "estSprouting": (TK, TK),
        # "sapSproutGrowth": (TK, TK),
        # "serotinyFormula": (TK, TK),
        # "serotinyFecundity": (TK, TK),
    }

    def __init__(self, excel_fname):
        self._fname = excel_fname
        self._sheets = {}

    def _get_param(self, short_name, k):
        sheet_name, col_name = self._schema[k]
        try:
            sh = self._sheets[sheet_name]
        except KeyError:
            # naively cache -- yay!
            sh = pd.read_excel(self._fname, sheet_name=sheet_name)
            self._sheets[sheet_name] = sh

        return sh.loc[sh.shortName == short_name].iloc[0][col_name]

    def get_species_params(self, short_name):
        return None
        # append isConiferous, isEvergreen, active, displayColor, LIPFile


def species_params(short_name):
    pass
