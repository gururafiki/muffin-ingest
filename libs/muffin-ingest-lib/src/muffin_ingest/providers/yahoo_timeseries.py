"""Yahoo's fundamentals-timeseries: a company's three statements, one request per frequency.

WHAT THIS REPLACES. The edge asked openbb for `equity/fundamental/{income,balance,cash}` one
statement and one frequency at a time — six calls per company — and openbb's yfinance adapter turns
each into this same endpoint with that statement's type list. Measured 2026-10-10: ONE request
carrying all three statements' 375 types answers (a 12 KB URL, 126-167 KB, 0.2-0.35 s), while 750
(both frequencies at once) is a **400** from Yahoo. So a statements visit is two requests.

THE ANSWER CARRIES ITS CURRENCY, AND openbb DROPPED IT. Every point states `currencyCode`, the
reporting currency (VOD.L EUR, 0992.HK USD, BHP.AX USD). yfinance keeps only `reportedValue.raw`, so
the 154,629 Yahoo statement rows the edge wrote all have `currency` null. Stage 1 here stores the
body, so nothing is dropped at all.

TWO FUNCTIONS, as in `yahoo_chart`: `fetch` touches the network and returns the body byte for byte;
`parse` is stage-2 code turning bytes into points.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from muffin_ingest import metrics, settings
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

#: THE TYPES ASKED FOR, per statement: yfinance 1.7.0's `yfinance.const.fundamentals_keys`
#: (`financials`, `balance-sheet`, `cash-flow`), COPIED rather than imported. The library carries no
#: provider hub (CI installs it without one), and a list read from whatever yfinance is installed
#: would change our requests on an upgrade without anything saying so. The image check compares the
#: two, so an upgrade that moves the list fails a PR instead of changing what is asked.
#:
#: The three lists share no key (measured), so a point belongs to exactly one statement.
STATEMENT_KEYS: dict[str, tuple[str, ...]] = {
    "income": (
        "TaxEffectOfUnusualItems",
        "TaxRateForCalcs",
        "NormalizedEBITDA",
        "NormalizedDilutedEPS",
        "NormalizedBasicEPS",
        "TotalUnusualItems",
        "TotalUnusualItemsExcludingGoodwill",
        "NetIncomeFromContinuingOperationNetMinorityInterest",
        "ReconciledDepreciation",
        "ReconciledCostOfRevenue",
        "EBITDA",
        "EBIT",
        "NetInterestIncome",
        "InterestExpense",
        "InterestIncome",
        "ContinuingAndDiscontinuedDilutedEPS",
        "ContinuingAndDiscontinuedBasicEPS",
        "NormalizedIncome",
        "NetIncomeFromContinuingAndDiscontinuedOperation",
        "TotalExpenses",
        "RentExpenseSupplemental",
        "ReportedNormalizedDilutedEPS",
        "ReportedNormalizedBasicEPS",
        "TotalOperatingIncomeAsReported",
        "DividendPerShare",
        "DilutedAverageShares",
        "BasicAverageShares",
        "DilutedEPS",
        "DilutedEPSOtherGainsLosses",
        "TaxLossCarryforwardDilutedEPS",
        "DilutedAccountingChange",
        "DilutedExtraordinary",
        "DilutedDiscontinuousOperations",
        "DilutedContinuousOperations",
        "BasicEPS",
        "BasicEPSOtherGainsLosses",
        "TaxLossCarryforwardBasicEPS",
        "BasicAccountingChange",
        "BasicExtraordinary",
        "BasicDiscontinuousOperations",
        "BasicContinuousOperations",
        "DilutedNIAvailtoComStockholders",
        "AverageDilutionEarnings",
        "NetIncomeCommonStockholders",
        "OtherunderPreferredStockDividend",
        "PreferredStockDividends",
        "NetIncome",
        "MinorityInterests",
        "NetIncomeIncludingNoncontrollingInterests",
        "NetIncomeFromTaxLossCarryforward",
        "NetIncomeExtraordinary",
        "NetIncomeDiscontinuousOperations",
        "NetIncomeContinuousOperations",
        "EarningsFromEquityInterestNetOfTax",
        "TaxProvision",
        "PretaxIncome",
        "OtherIncomeExpense",
        "OtherNonOperatingIncomeExpenses",
        "SpecialIncomeCharges",
        "GainOnSaleOfPPE",
        "GainOnSaleOfBusiness",
        "OtherSpecialCharges",
        "WriteOff",
        "ImpairmentOfCapitalAssets",
        "RestructuringAndMergernAcquisition",
        "SecuritiesAmortization",
        "EarningsFromEquityInterest",
        "GainOnSaleOfSecurity",
        "NetNonOperatingInterestIncomeExpense",
        "TotalOtherFinanceCost",
        "InterestExpenseNonOperating",
        "InterestIncomeNonOperating",
        "OperatingIncome",
        "OperatingExpense",
        "OtherOperatingExpenses",
        "OtherTaxes",
        "ProvisionForDoubtfulAccounts",
        "DepreciationAmortizationDepletionIncomeStatement",
        "DepletionIncomeStatement",
        "DepreciationAndAmortizationInIncomeStatement",
        "Amortization",
        "AmortizationOfIntangiblesIncomeStatement",
        "DepreciationIncomeStatement",
        "ResearchAndDevelopment",
        "SellingGeneralAndAdministration",
        "SellingAndMarketingExpense",
        "GeneralAndAdministrativeExpense",
        "OtherGandA",
        "InsuranceAndClaims",
        "RentAndLandingFees",
        "SalariesAndWages",
        "GrossProfit",
        "CostOfRevenue",
        "TotalRevenue",
        "ExciseTaxes",
        "OperatingRevenue",
        "LossAdjustmentExpense",
        "NetPolicyholderBenefitsAndClaims",
        "PolicyholderBenefitsGross",
        "PolicyholderBenefitsCeded",
        "OccupancyAndEquipment",
        "ProfessionalExpenseAndContractServicesExpense",
        "OtherNonInterestExpense",
    ),
    "balance": (
        "TreasurySharesNumber",
        "PreferredSharesNumber",
        "OrdinarySharesNumber",
        "ShareIssued",
        "NetDebt",
        "TotalDebt",
        "TangibleBookValue",
        "InvestedCapital",
        "WorkingCapital",
        "NetTangibleAssets",
        "CapitalLeaseObligations",
        "CommonStockEquity",
        "PreferredStockEquity",
        "TotalCapitalization",
        "TotalEquityGrossMinorityInterest",
        "MinorityInterest",
        "StockholdersEquity",
        "OtherEquityInterest",
        "GainsLossesNotAffectingRetainedEarnings",
        "OtherEquityAdjustments",
        "FixedAssetsRevaluationReserve",
        "ForeignCurrencyTranslationAdjustments",
        "MinimumPensionLiabilities",
        "UnrealizedGainLoss",
        "TreasuryStock",
        "RetainedEarnings",
        "AdditionalPaidInCapital",
        "CapitalStock",
        "OtherCapitalStock",
        "CommonStock",
        "PreferredStock",
        "TotalPartnershipCapital",
        "GeneralPartnershipCapital",
        "LimitedPartnershipCapital",
        "TotalLiabilitiesNetMinorityInterest",
        "TotalNonCurrentLiabilitiesNetMinorityInterest",
        "OtherNonCurrentLiabilities",
        "LiabilitiesHeldforSaleNonCurrent",
        "RestrictedCommonStock",
        "PreferredSecuritiesOutsideStockEquity",
        "DerivativeProductLiabilities",
        "EmployeeBenefits",
        "NonCurrentPensionAndOtherPostretirementBenefitPlans",
        "NonCurrentAccruedExpenses",
        "DuetoRelatedPartiesNonCurrent",
        "TradeandOtherPayablesNonCurrent",
        "NonCurrentDeferredLiabilities",
        "NonCurrentDeferredRevenue",
        "NonCurrentDeferredTaxesLiabilities",
        "LongTermDebtAndCapitalLeaseObligation",
        "LongTermCapitalLeaseObligation",
        "LongTermDebt",
        "LongTermProvisions",
        "CurrentLiabilities",
        "OtherCurrentLiabilities",
        "CurrentDeferredLiabilities",
        "CurrentDeferredRevenue",
        "CurrentDeferredTaxesLiabilities",
        "CurrentDebtAndCapitalLeaseObligation",
        "CurrentCapitalLeaseObligation",
        "CurrentDebt",
        "OtherCurrentBorrowings",
        "LineOfCredit",
        "CommercialPaper",
        "CurrentNotesPayable",
        "PensionandOtherPostRetirementBenefitPlansCurrent",
        "CurrentProvisions",
        "PayablesAndAccruedExpenses",
        "CurrentAccruedExpenses",
        "InterestPayable",
        "Payables",
        "OtherPayable",
        "DuetoRelatedPartiesCurrent",
        "DividendsPayable",
        "TotalTaxPayable",
        "IncomeTaxPayable",
        "AccountsPayable",
        "TotalAssets",
        "TotalNonCurrentAssets",
        "OtherNonCurrentAssets",
        "DefinedPensionBenefit",
        "NonCurrentPrepaidAssets",
        "NonCurrentDeferredAssets",
        "NonCurrentDeferredTaxesAssets",
        "DuefromRelatedPartiesNonCurrent",
        "NonCurrentNoteReceivables",
        "NonCurrentAccountsReceivable",
        "FinancialAssets",
        "InvestmentsAndAdvances",
        "OtherInvestments",
        "InvestmentinFinancialAssets",
        "HeldToMaturitySecurities",
        "AvailableForSaleSecurities",
        "FinancialAssetsDesignatedasFairValueThroughProfitorLossTotal",
        "TradingSecurities",
        "LongTermEquityInvestment",
        "InvestmentsinJointVenturesatCost",
        "InvestmentsInOtherVenturesUnderEquityMethod",
        "InvestmentsinAssociatesatCost",
        "InvestmentsinSubsidiariesatCost",
        "InvestmentProperties",
        "GoodwillAndOtherIntangibleAssets",
        "OtherIntangibleAssets",
        "Goodwill",
        "NetPPE",
        "AccumulatedDepreciation",
        "GrossPPE",
        "Leases",
        "ConstructionInProgress",
        "OtherProperties",
        "MachineryFurnitureEquipment",
        "BuildingsAndImprovements",
        "LandAndImprovements",
        "Properties",
        "CurrentAssets",
        "OtherCurrentAssets",
        "HedgingAssetsCurrent",
        "AssetsHeldForSaleCurrent",
        "CurrentDeferredAssets",
        "CurrentDeferredTaxesAssets",
        "RestrictedCash",
        "PrepaidAssets",
        "Inventory",
        "InventoriesAdjustmentsAllowances",
        "OtherInventories",
        "FinishedGoods",
        "WorkInProcess",
        "RawMaterials",
        "Receivables",
        "ReceivablesAdjustmentsAllowances",
        "OtherReceivables",
        "DuefromRelatedPartiesCurrent",
        "TaxesReceivable",
        "AccruedInterestReceivable",
        "NotesReceivable",
        "LoansReceivable",
        "AccountsReceivable",
        "AllowanceForDoubtfulAccountsReceivable",
        "GrossAccountsReceivable",
        "CashCashEquivalentsAndShortTermInvestments",
        "OtherShortTermInvestments",
        "CashAndCashEquivalents",
        "CashEquivalents",
        "CashFinancial",
        "CashCashEquivalentsAndFederalFundsSold",
        "FixedMaturityInvestments",
        "EquityInvestments",
        "NetLoan",
        "DeferredAssets",
    ),
    "cash": (
        "ForeignSales",
        "DomesticSales",
        "AdjustedGeographySegmentData",
        "FreeCashFlow",
        "RepurchaseOfCapitalStock",
        "RepaymentOfDebt",
        "IssuanceOfDebt",
        "IssuanceOfCapitalStock",
        "CapitalExpenditure",
        "InterestPaidSupplementalData",
        "IncomeTaxPaidSupplementalData",
        "EndCashPosition",
        "OtherCashAdjustmentOutsideChangeinCash",
        "BeginningCashPosition",
        "EffectOfExchangeRateChanges",
        "ChangesInCash",
        "OtherCashAdjustmentInsideChangeinCash",
        "CashFlowFromDiscontinuedOperation",
        "FinancingCashFlow",
        "CashFromDiscontinuedFinancingActivities",
        "CashFlowFromContinuingFinancingActivities",
        "NetOtherFinancingCharges",
        "InterestPaidCFF",
        "ProceedsFromStockOptionExercised",
        "CashDividendsPaid",
        "PreferredStockDividendPaid",
        "CommonStockDividendPaid",
        "NetPreferredStockIssuance",
        "PreferredStockPayments",
        "PreferredStockIssuance",
        "NetCommonStockIssuance",
        "CommonStockPayments",
        "CommonStockIssuance",
        "NetIssuancePaymentsOfDebt",
        "NetShortTermDebtIssuance",
        "ShortTermDebtPayments",
        "ShortTermDebtIssuance",
        "NetLongTermDebtIssuance",
        "LongTermDebtPayments",
        "LongTermDebtIssuance",
        "InvestingCashFlow",
        "CashFromDiscontinuedInvestingActivities",
        "CashFlowFromContinuingInvestingActivities",
        "NetOtherInvestingChanges",
        "InterestReceivedCFI",
        "DividendsReceivedCFI",
        "NetInvestmentPurchaseAndSale",
        "SaleOfInvestment",
        "PurchaseOfInvestment",
        "NetInvestmentPropertiesPurchaseAndSale",
        "SaleOfInvestmentProperties",
        "PurchaseOfInvestmentProperties",
        "NetBusinessPurchaseAndSale",
        "SaleOfBusiness",
        "PurchaseOfBusiness",
        "NetIntangiblesPurchaseAndSale",
        "SaleOfIntangibles",
        "PurchaseOfIntangibles",
        "NetPPEPurchaseAndSale",
        "SaleOfPPE",
        "PurchaseOfPPE",
        "CapitalExpenditureReported",
        "OperatingCashFlow",
        "CashFromDiscontinuedOperatingActivities",
        "CashFlowFromContinuingOperatingActivities",
        "TaxesRefundPaid",
        "InterestReceivedCFO",
        "InterestPaidCFO",
        "DividendReceivedCFO",
        "DividendPaidCFO",
        "ChangeInWorkingCapital",
        "ChangeInOtherWorkingCapital",
        "ChangeInOtherCurrentLiabilities",
        "ChangeInOtherCurrentAssets",
        "ChangeInPayablesAndAccruedExpense",
        "ChangeInAccruedExpense",
        "ChangeInInterestPayable",
        "ChangeInPayable",
        "ChangeInDividendPayable",
        "ChangeInAccountPayable",
        "ChangeInTaxPayable",
        "ChangeInIncomeTaxPayable",
        "ChangeInPrepaidAssets",
        "ChangeInInventory",
        "ChangeInReceivables",
        "ChangesInAccountReceivables",
        "OtherNonCashItems",
        "ExcessTaxBenefitFromStockBasedCompensation",
        "StockBasedCompensation",
        "UnrealizedGainLossOnInvestmentSecurities",
        "ProvisionandWriteOffofAssets",
        "AssetImpairmentCharge",
        "AmortizationOfSecurities",
        "DeferredTax",
        "DeferredIncomeTax",
        "DepreciationAmortizationDepletion",
        "Depletion",
        "DepreciationAndAmortization",
        "AmortizationCashFlow",
        "AmortizationOfIntangibles",
        "Depreciation",
        "OperatingGainsLosses",
        "PensionAndEmployeeBenefitExpense",
        "EarningsLossesFromEquityInvestments",
        "GainLossOnInvestmentSecurities",
        "NetForeignCurrencyExchangeGainLoss",
        "GainLossOnSaleOfPPE",
        "GainLossOnSaleOfBusiness",
        "NetIncomeFromContinuingOperations",
        "CashFlowsfromusedinOperatingActivitiesDirect",
        "TaxesRefundPaidDirect",
        "InterestReceivedDirect",
        "InterestPaidDirect",
        "DividendsReceivedDirect",
        "DividendsPaidDirect",
        "ClassesofCashPayments",
        "OtherCashPaymentsfromOperatingActivities",
        "PaymentsonBehalfofEmployees",
        "PaymentstoSuppliersforGoodsandServices",
        "ClassesofCashReceiptsfromOperatingActivities",
        "OtherCashReceiptsfromOperatingActivities",
        "ReceiptsfromGovernmentGrants",
        "ReceiptsfromCustomers",
    ),
}

#: The statements in the order their types are asked for. The order is part of the URL.
STATEMENTS: tuple[str, ...] = ("income", "balance", "cash")

#: Our frequency names and the prefix Yahoo puts on each type.
FREQUENCIES: tuple[str, ...] = ("annual", "quarterly")

#: yfinance's own lower bound, kept so the request is the one the edge's provider made. Yahoo
#: returns at most ~5 fiscal years or ~6 quarters whatever it says.
HISTORY_FROM = date(2016, 12, 31)


def types(frequency: str) -> list[str]:
    """Every type for one frequency, all three statements, in a fixed order."""
    if frequency not in FREQUENCIES:
        raise ValueError(f"frequency must be one of {FREQUENCIES}, got {frequency!r}")
    return [f"{frequency}{key}" for statement in STATEMENTS for key in STATEMENT_KEYS[statement]]


#: Which statement a key (a type without its frequency prefix) belongs to.
STATEMENT_OF: dict[str, str] = {
    key: statement for statement in STATEMENTS for key in STATEMENT_KEYS[statement]
}


def fetch(symbol: str, frequency: str, *, today: date, timeout_s: float = 30.0) -> Document:
    """The response body for one frequency, exactly as Yahoo sent it. Raises on a refusal.

    The window ends the day after `today`, so a period reported today is inside it, and starts at
    `HISTORY_FROM`; both are whole days, so the same visit on the same day is the same URL.
    """
    base = settings.provider_base("yahoo", yahoo_chart.REAL_ORIGIN)
    path = yahoo_chart.path_segment(symbol)
    url = f"{base}/ws/fundamentals-timeseries/v1/finance/timeseries/{path}"
    params = {
        "symbol": symbol,
        "type": ",".join(types(frequency)),
        "period1": str(yahoo_chart.day_start(HISTORY_FROM)),
        "period2": str(yahoo_chart.day_start(today + timedelta(days=1))),
    }
    refusal: yahoo_chart.YahooRefused | None = None
    with metrics.request("yahoo-timeseries") as outcome:
        try:
            response = httpx.get(
                url,
                params=params,
                headers={"User-Agent": yahoo_chart.BROWSER_UA},
                timeout=timeout_s,
            )
        except httpx.HTTPError as exc:
            raise yahoo_chart.YahooRefused(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code != 200:
            # BY STATUS, NOT BY A MESSAGE NAMING THE SYMBOL: `4290.T` contains `429`.
            refusal = yahoo_chart.refusal(response.status_code, "timeseries")
            outcome["outcome"] = "refused"
    if refusal is not None:
        raise refusal
    return Document(
        url=str(httpx.URL(url, params=params)),
        body=response.content,
        content_type=response.headers.get("content-type", "application/json"),
        fetched_at=datetime.now(UTC),
    )


# --- stage 2 -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Point:
    """One reported value: which line, which period, in which currency, as Yahoo stated it."""

    key: str
    statement: str
    as_of: date
    #: Yahoo's own label, `12M` or `3M`. NOT the period's type: a half-year arrives as `3M`
    #: (VOD.L, NESN.SW, BHP.AX), which is why `facets.statements` decides the type itself.
    period_type: str | None
    currency: str | None
    value: float | None


@dataclass
class Timeseries:
    points: list[Point] = field(default_factory=list)
    #: Types the body carried that are in none of the three lists — a key Yahoo added. Counted,
    #: because the point cannot be filed under a statement, and the body still holds it.
    unknown_keys: set[str] = field(default_factory=set)
    error: dict[str, Any] | None = None


def parse(body: bytes, frequency: str) -> Timeseries:
    """Bytes to points. An unreadable body raises; an empty answer is an empty `Timeseries`."""
    try:
        parsed: Any = json.loads(body)
    except ValueError as exc:
        raise yahoo_chart.YahooRefused(f"unparseable timeseries body: {exc}") from exc
    block = parsed.get("timeseries") if isinstance(parsed, dict) else None
    if not isinstance(block, dict):
        raise yahoo_chart.YahooRefused("timeseries body has no timeseries object")
    out = Timeseries()
    if block.get("error"):
        error = block["error"]
        out.error = error if isinstance(error, dict) else {"description": str(error)}
        return out
    for result in block.get("result") or []:
        if not isinstance(result, dict):
            continue
        type_names = (result.get("meta") or {}).get("type") or []
        type_name = type_names[0] if type_names and isinstance(type_names[0], str) else None
        if type_name is None or not type_name.startswith(frequency):
            continue
        key = type_name[len(frequency) :]
        statement = STATEMENT_OF.get(key)
        points = result.get(type_name)
        if not isinstance(points, list):
            continue
        if statement is None:
            out.unknown_keys.add(key)
            continue
        for point in points:
            if not isinstance(point, dict):
                continue
            try:
                as_of = date.fromisoformat(str(point.get("asOfDate")))
            except ValueError:
                continue
            reported = point.get("reportedValue")
            raw = reported.get("raw") if isinstance(reported, dict) else None
            out.points.append(
                Point(
                    key=key,
                    statement=statement,
                    as_of=as_of,
                    period_type=point.get("periodType")
                    if isinstance(point.get("periodType"), str)
                    else None,
                    currency=point.get("currencyCode")
                    if isinstance(point.get("currencyCode"), str)
                    else None,
                    value=float(raw)
                    if isinstance(raw, int | float) and not isinstance(raw, bool)
                    else None,
                )
            )
    return out
