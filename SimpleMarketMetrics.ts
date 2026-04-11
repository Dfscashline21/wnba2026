# Simple Market Metrics Trading System
# Converted from PineScript v5

declare lower;

# Inputs for Buy/Sell Signals
input enableBuySellSignals = yes;
input enableBuySellPrice = yes;
input buySignalColor = Color.GREEN;
input sellSignalColor = Color.RED;

# Inputs for Trend Settings
input enableBgColor = yes;
input bgTrendBullishCol = Color.GREEN;
input bgTrendBearishCol = Color.RED;

# Inputs for MFI
input mfiLength = 14;
input mfiOverbought = 80;
input mfiOversold = 20;

# Variables
def srcClose = close;
def srcHigh = high;
def srcLow = low;
def srcOpen = open;

# Trend Direction Calculation
def Up = (srcHigh + srcLow) / 2 - (1.3 * Average(TrueRange(high, close, low), 8));
def Dn = (srcHigh + srcLow) / 2 + (1.3 * Average(TrueRange(high, close, low), 8));

def trendUp;
def trendDown;
def trendSwitch;

if (srcClose[1] > trendUp[1]) {
    trendUp = Max(Up, trendUp[1]);
} else {
    trendUp = Up;
}

if (srcClose[1] < trendDown[1]) {
    trendDown = Min(Dn, trendDown[1]);
} else {
    trendDown = Dn;
}

trendSwitch = if srcClose > trendDown[1] then 1 else if srcClose < trendUp[1] then -1 else trendSwitch[1];
def trendDirection = if trendSwitch == 1 then trendUp else trendDown;
def bullishTrend = trendDirection == trendUp;
def bearishTrend = trendDirection == trendDown;

# Background Trend Color
def backgroundTrendColor;
if (trendDirection == trendUp) {
    backgroundTrendColor = bgTrendBullishCol;
} else if (trendDirection == trendDown) {
    backgroundTrendColor = bgTrendBearishCol;
} else {
    backgroundTrendColor = backgroundTrendColor[1];
}

# Plot Background
plot background = if enableBgColor then backgroundTrendColor else Double.NaN;
background.SetPaintingStrategy(PaintingStrategy.SQUARE);
background.SetLineWeight(1);

# Buy/Sell Signals
def strongBullishCandle = srcClose > srcOpen and Floor(srcOpen) == Floor(srcLow);
def strongBearishCandle = srcClose < srcOpen and Floor(srcOpen) == Floor(srcHigh);

def buySignal = bullishTrend and backgroundTrendColor == bgTrendBullishCol and strongBullishCandle;
def sellSignal = bearishTrend and backgroundTrendColor == bgTrendBearishCol and strongBearishCandle;

# Plot Signals
plot buyPlot = if enableBuySellSignals and buySignal then low else Double.NaN;
buyPlot.SetPaintingStrategy(PaintingStrategy.ARROW_UP);
buyPlot.SetLineWeight(3);
buyPlot.AssignValueColor(buySignalColor);

plot sellPlot = if enableBuySellSignals and sellSignal then high else Double.NaN;
sellPlot.SetPaintingStrategy(PaintingStrategy.ARROW_DOWN);
sellPlot.SetLineWeight(3);
sellPlot.AssignValueColor(sellSignalColor);

# Add Labels for Buy/Sell Signals
AddChartBubble(enableBuySellSignals and buySignal, low, "Buy" + (if enableBuySellPrice then "\n$" + close else ""), buySignalColor, yes);
AddChartBubble(enableBuySellSignals and sellSignal, high, "Sell" + (if enableBuySellPrice then "\n$" + close else ""), sellSignalColor, no);

# Money Flow Index (MFI)
def typicalPrice = (high + low + close) / 3;
def moneyFlow = typicalPrice * volume;
def positiveFlow = if typicalPrice > typicalPrice[1] then moneyFlow else 0;
def negativeFlow = if typicalPrice < typicalPrice[1] then moneyFlow else 0;

def positiveMF = Average(positiveFlow, mfiLength);
def negativeMF = Average(negativeFlow, mfiLength);

def mfi = 100 - (100 / (1 + positiveMF / negativeMF));

# Plot MFI
plot mfiPlot = mfi;
mfiPlot.SetPaintingStrategy(PaintingStrategy.LINE);
mfiPlot.SetLineWeight(2);
mfiPlot.AssignValueColor(if mfi > mfiOverbought then Color.RED else if mfi < mfiOversold then Color.GREEN else Color.YELLOW);

# Add MFI reference lines
plot overbought = mfiOverbought;
plot oversold = mfiOversold;
overbought.SetPaintingStrategy(PaintingStrategy.DASHES);
oversold.SetPaintingStrategy(PaintingStrategy.DASHES);
overbought.SetDefaultColor(Color.RED);
oversold.SetDefaultColor(Color.GREEN);

# Add alerts
Alert(buySignal, "Buy Signal", Alert.BAR, Sound.Ding);
Alert(sellSignal, "Sell Signal", Alert.BAR, Sound.Ding);
Alert(mfi crosses above mfiOversold, "MFI Oversold", Alert.BAR, Sound.Ding);
Alert(mfi crosses below mfiOverbought, "MFI Overbought", Alert.BAR, Sound.Ding); 