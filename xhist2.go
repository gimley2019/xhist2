package main

/*
	-- 10.27.24 --
	This module will create and maintain stockhistory collection that will contain ever
	growing number of meta data about that stock to be used by xmod3 rules and multiple
	future modules as well as analysis.

	This module is intened to run daily and ad hoc. It is expected that scope of stockhistory record
	will continue to grow and hence this module will have ability to create records for the next day
	as well as to update existing records without the need to re-run the entire history from the beginning

*/
import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"mylib2"
	"sort"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ExpiryBucket maps each Expiration Date to particular bucket Value

//TrendRec stores the results of the analysis for each date

// PrintTrendMatrix prints out trendmatrix into a report
const TDAYSANNUALY int = 252
const TDAYSMONTHLY int = 22
const TDAYSQUARTERLY int = 63

func main() {
	var lookback int
	var err error
	var SymbolList []string
	var lookbackFilter bson.D

	fmt.Println("WELCOME TO STOCK HISTORY")

	envFlag := flag.String("env", "DEV", "Environment: dev or prod")
	symbolFlag := flag.String("symbol", "0", "Symbol: default is 0")
	lookBackFlag := flag.String("lookback", "AUTO", "default lookback is AUTO")
	threadsFlag := flag.String("threads", "1", "default is 1")

	flag.Parse()

	fmt.Printf("Running with: %v %v %v %v\n", *envFlag, *symbolFlag, *lookBackFlag, *threadsFlag)
	// Initialize the database connection
	mylib2.InitDB(*envFlag)

	lookbackStr := *lookBackFlag
	threads, err := strconv.Atoi(*threadsFlag)
	if err != nil {
		log.Panic("invalid thread value")
	}
	symbol := *symbolFlag

	if symbol == "0" {
		SymbolList, err = mylib2.GetWeeklyStocks()
		if err != nil {
			log.Fatal("Can't get weekly stocks")
		}
		lookbackFilter = bson.D{{}}
	} else {
		lookbackFilter = bson.D{{Key: "underlying", Value: symbol}}
		SymbolList = append(SymbolList, symbol)
	}

	if lookbackStr != "AUTO" {
		lookback, err = strconv.Atoi(lookbackStr)
		if err != nil {
			fmt.Printf("INVALID lookback period %v\n", lookbackStr)
			return
		}
	} else {
		maxDateOptions, err := mylib2.FindMaxDateOptions()
		if err != nil {
			log.Fatal("can't find latest date")
		}
		maxDateStockHistory, err := mylib2.FindMaxDateStockHistory(lookbackFilter)
		if err != nil {
			log.Fatal("can't find latest date")
		}
		diff := maxDateOptions.Sub(maxDateStockHistory)
		lookback = int(diff.Hours() / 24)
	}
	fmt.Printf("Using %v days for lookback\n", lookback)

	jobs := make(chan string, 10000)
	results := make(chan bool, 10000)

	for i := 0; i < threads; i++ {
		go loader(jobs, results, lookback)
	}

	for _, symbol := range SymbolList {
		jobs <- symbol
	}

	close(jobs)

	for i := 0; i < len(SymbolList); i++ {
		res := <-results
		if !res {
			fmt.Println("Error processing symbol")
		}
	}

	fmt.Println("Done")

}
func ProcessSymbol(underlying string, lookback int) bool {
	var status bool = true

	History := VolTrend(underlying, lookback)

	if len(History) == 0 {
		fmt.Printf("No option data for %v. Skipping symbol\n", underlying)
		return false
	}
	/*
		Now we enrich the basic VolTrend with earinings data,
		IV pct data, stock hist vol, stock move data, etc..
	*/
	_, err := mylib2.InsertStockHistoryRecsBulk(History)
	if err != nil {
		fmt.Println("Trouble inserting stock history data")
		fmt.Println(err)
		status = false
	}

	//Next lets add ratings
	//AddRatings(underlying)
	AddIVpercentiles(underlying)
	AddExpectedMoves(underlying)
	AddExpectedMovePercentiles(underlying)
	fmt.Printf("Finished %v %v\n", underlying, time.Now())
	return status
}
func loader(jobs <-chan string, results chan<- bool, lookback int) {
	for symbol := range jobs {
		results <- ProcessSymbol(symbol, lookback)
	}
}
func AddRatings(symbol string) {
	fmt.Printf("Adding Ratings For: %v\n", symbol)
	filter := bson.D{{Key: "underlying", Value: symbol}, {Key: "ratings.symbol", Value: ""}}
	stockHist, hstatus := mylib2.GetStockHistoryData(filter)
	if !hstatus {
		fmt.Printf("no stock history for %v. skipping symbol\n", symbol)
		return
	}

	ratings := mylib2.GetRatingsHistoryForSymbol(symbol)
	for i, hrec := range stockHist {
		for _, rrec := range ratings {
			if hrec.Datadate.Equal(rrec.DateDt) {
				stockHist[i].Ratings = rrec
				filter := bson.D{{Key: "underlying", Value: hrec.Underlying}, {Key: "datadate", Value: hrec.Datadate}}
				update := bson.D{{Key: "$set", Value: bson.D{{Key: "ratings", Value: rrec}}}}
				matched, updated := mylib2.UpdateOne("StockHistory", filter, update)
				if matched != 1 || updated != 1 {
					fmt.Printf("Issue Updating document %v\n", hrec)
				}
				break
			}
		}
	}

}

func VolTrend(underlying string, lookback int) []mylib2.StockHistory {
	var History []mylib2.StockHistory
	var status bool
	fmt.Println("====================================")
	fmt.Printf("%v %v\n", underlying, time.Now())

	DateList := mylib2.GetDistinctDatesFromOptions2(underlying)

	sort.Slice(DateList, func(i, j int) bool {
		return DateList[i].Ddate.Before(DateList[j].Ddate)
	})
	if len(DateList) > lookback {
		DateList = DateList[len(DateList)-lookback:]
	}

	//now get dates that are already there
	trendDates, err := mylib2.GetStockHistoryDates(underlying)
	if err != nil {
		fmt.Println("cant get trend dates")
	}
	if len(trendDates) > 0 {
		DateList, status = CompareDateLists2(DateList, trendDates)
		if !status {
			fmt.Println("Issue with the list compare")
		}
	}

	fmt.Printf("Adding %v days\n", len(DateList))
	fmt.Println()
	for _, thisDate := range DateList {
		var thisHistRec mylib2.StockHistory
		//if thisDate.Ddate.Format("2006-01-02") == "2025-05-15" {
		//	fmt.Println("STOP FOR DEBUG")
		//}
		thisHistRec.Underlying = underlying
		thisHistRec.Datadate = thisDate.Ddate
		thisHistRec.WeekDay = thisDate.WeekDay
		expirations, err := mylib2.GetExpirations(underlying, thisDate.Ddate)
		if err != nil {
			break
		}
		for _, thisExpiry := range expirations {
			// determine what bucket does expiration fall
			BucketRec := mylib2.DetermineBucket(thisDate, thisExpiry)
			optList, err := mylib2.GetOptionList(underlying, thisExpiry.Ddate, "put", thisDate.Ddate)
			if err != nil {
				break
			}
			ATMOpt, err := mylib2.FindATCOption(optList)
			if err != nil {
				break
			}
			if thisHistRec.UnderlyingPrice == 0 {
				thisHistRec.UnderlyingPrice = ATMOpt.UnderlyingPrice
			}

			switch BucketRec.Bucket {
			case 5:
				if thisHistRec.Day5 == 0 {
					thisHistRec.Day5 = ATMOpt.IV
					thisHistRec.Day5Vega = ATMOpt.Vega
					thisHistRec.Day5Gamma = ATMOpt.Gamma
					thisHistRec.Day5Delta = ATMOpt.Delta
					thisHistRec.Day5Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day5ATMSrike = ATMOpt.Strike
					thisHistRec.Day5Volume = ATMOpt.Volume
					thisHistRec.Day5OpenInterest = ATMOpt.OpenInterest

				}
			case 10:
				if thisHistRec.Day10 == 0 {
					thisHistRec.Day10 = ATMOpt.IV
					thisHistRec.Day10Vega = ATMOpt.Vega
					thisHistRec.Day10Gamma = ATMOpt.Gamma
					thisHistRec.Day10Delta = ATMOpt.Delta
					thisHistRec.Day10Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day10ATMSrike = ATMOpt.Strike
					thisHistRec.Day10Volume = ATMOpt.Volume
					thisHistRec.Day10OpenInterest = ATMOpt.OpenInterest

				}
			case 30:
				if thisHistRec.Day30 == 0 {
					thisHistRec.Day30 = ATMOpt.IV
					thisHistRec.Day30Vega = ATMOpt.Vega
					thisHistRec.Day30Gamma = ATMOpt.Gamma
					thisHistRec.Day30Delta = ATMOpt.Delta
					thisHistRec.Day30Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day30ATMSrike = ATMOpt.Strike
					thisHistRec.Day30Volume = ATMOpt.Volume
					thisHistRec.Day30OpenInterest = ATMOpt.OpenInterest
				}

			case 60:
				if thisHistRec.Day60 == 0 {
					thisHistRec.Day60 = ATMOpt.IV
					thisHistRec.Day60Vega = ATMOpt.Vega
					thisHistRec.Day60Gamma = ATMOpt.Gamma
					thisHistRec.Day60Delta = ATMOpt.Delta
					thisHistRec.Day60Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day60ATMSrike = ATMOpt.Strike
					thisHistRec.Day60Volume = ATMOpt.Volume
					thisHistRec.Day60OpenInterest = ATMOpt.OpenInterest
				}
			case 90:
				if thisHistRec.Day90 == 0 {
					thisHistRec.Day90 = ATMOpt.IV
					thisHistRec.Day90Vega = ATMOpt.Vega
					thisHistRec.Day90Gamma = ATMOpt.Gamma
					thisHistRec.Day90Delta = ATMOpt.Delta
					thisHistRec.Day90Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day90ATMSrike = ATMOpt.Strike
					thisHistRec.Day90Volume = ATMOpt.Volume
					thisHistRec.Day90OpenInterest = ATMOpt.OpenInterest
				}

			case 120:
				if thisHistRec.Day120 == 0 {
					thisHistRec.Day120 = ATMOpt.IV
					thisHistRec.Day120Vega = ATMOpt.Vega
					thisHistRec.Day120Gamma = ATMOpt.Gamma
					thisHistRec.Day120Delta = ATMOpt.Delta
					thisHistRec.Day120Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day120ATMSrike = ATMOpt.Strike
					thisHistRec.Day120Volume = ATMOpt.Volume
					thisHistRec.Day120OpenInterest = ATMOpt.OpenInterest
				}

			case 180:
				if thisHistRec.Day180 == 0 {
					thisHistRec.Day180 = ATMOpt.IV
					thisHistRec.Day180Vega = ATMOpt.Vega
					thisHistRec.Day180Gamma = ATMOpt.Gamma
					thisHistRec.Day180Delta = ATMOpt.Delta
					thisHistRec.Day180Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day180ATMSrike = ATMOpt.Strike
					thisHistRec.Day180Volume = ATMOpt.Volume
					thisHistRec.Day180OpenInterest = ATMOpt.OpenInterest
				}

			case 240:
				if thisHistRec.Day240 == 0 {
					thisHistRec.Day240 = ATMOpt.IV
					thisHistRec.Day240Vega = ATMOpt.Vega
					thisHistRec.Day240Gamma = ATMOpt.Gamma
					thisHistRec.Day240Delta = ATMOpt.Delta
					thisHistRec.Day240Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day240ATMSrike = ATMOpt.Strike
					thisHistRec.Day240Volume = ATMOpt.Volume
					thisHistRec.Day240OpenInterest = ATMOpt.OpenInterest
				}

			case 365:
				if thisHistRec.Day365 == 0 {
					thisHistRec.Day365 = ATMOpt.IV
					thisHistRec.Day365Vega = ATMOpt.Vega
					thisHistRec.Day365Gamma = ATMOpt.Gamma
					thisHistRec.Day365Delta = ATMOpt.Delta
					thisHistRec.Day365Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day365ATMSrike = ATMOpt.Strike
					thisHistRec.Day365Volume = ATMOpt.Volume
					thisHistRec.Day365OpenInterest = ATMOpt.OpenInterest
				}
			case 480:
				if thisHistRec.Day480 == 0 {
					thisHistRec.Day480 = ATMOpt.IV
					thisHistRec.Day480Vega = ATMOpt.Vega
					thisHistRec.Day480Gamma = ATMOpt.Gamma
					thisHistRec.Day480Delta = ATMOpt.Delta
					thisHistRec.Day480Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day480ATMSrike = ATMOpt.Strike
					thisHistRec.Day480Volume = ATMOpt.Volume
					thisHistRec.Day480OpenInterest = ATMOpt.OpenInterest
				}
			case 600:
				if thisHistRec.Day600 == 0 {
					thisHistRec.Day600 = ATMOpt.IV
					thisHistRec.Day600Vega = ATMOpt.Vega
					thisHistRec.Day600Gamma = ATMOpt.Gamma
					thisHistRec.Day600Delta = ATMOpt.Delta
					thisHistRec.Day600Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day600ATMSrike = ATMOpt.Strike
					thisHistRec.Day600Volume = ATMOpt.Volume
					thisHistRec.Day600OpenInterest = ATMOpt.OpenInterest
				}
			case 730:
				if thisHistRec.Day730 == 0 {
					thisHistRec.Day730 = ATMOpt.IV
					thisHistRec.Day730Vega = ATMOpt.Vega
					thisHistRec.Day730Gamma = ATMOpt.Gamma
					thisHistRec.Day730Delta = ATMOpt.Delta
					thisHistRec.Day730Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day730ATMSrike = ATMOpt.Strike
					thisHistRec.Day730Volume = ATMOpt.Volume
					thisHistRec.Day730OpenInterest = ATMOpt.OpenInterest
				}
			case 850:
				if thisHistRec.Day850 == 0 {
					thisHistRec.Day850 = ATMOpt.IV
					thisHistRec.Day850Vega = ATMOpt.Vega
					thisHistRec.Day850Gamma = ATMOpt.Gamma
					thisHistRec.Day850Delta = ATMOpt.Delta
					thisHistRec.Day850Expiration = BucketRec.Expiry.Ddate
					thisHistRec.Day850ATMSrike = ATMOpt.Strike
					thisHistRec.Day850Volume = ATMOpt.Volume
					thisHistRec.Day850OpenInterest = ATMOpt.OpenInterest
				}
			}

		}
		// Fill the gaps
		FillGaps(&thisHistRec)
		// Get earnings info
		thisHistRec.LastEarningsDate, thisHistRec.NextEarningsDate, thisHistRec.LastEarningsSurprise, thisHistRec.IsEarnings = GetEarnings(thisHistRec.Underlying, thisDate.Ddate)
		// Get DCF Info
		//thisHistRec.DCF = fmplib.GetDCF(thisHistRec.Underlying)
		//thisRatingHistory := fmplib.GetRatingsHistoryForSymbol(thisHistRec.Underlying)
		//fmt.Println(thisRatingHistory)
		// Get
		if !mylib2.IsThereStockHistRec(thisHistRec.Underlying, thisDate.Ddate) {
			History = append(History, thisHistRec)
			//fmt.Printf("adding %v %v\n", thisHistRec.Underlying, thisDate.Ddate)
		} else {
			fmt.Printf("skipping %v %v\n", thisHistRec.Underlying, thisDate.Ddate)
		}
		/*
			if math.Mod(float64(i), 100) == 0 {
				fmt.Printf("Processed %v recs. %v\n", i, time.Now())
			}
		*/
	}
	return History
}
func FillGaps(hrec *mylib2.StockHistory) {
	if hrec.Day365 == 0 {
		if hrec.Day480 > 0 {
			(*hrec).Day365 = hrec.Day480
		} else {
			if hrec.Day240 > 0 {
				(*hrec).Day365 = hrec.Day240
			} else {
				if hrec.Day600 > 0 {
					(*hrec).Day365 = hrec.Day600
				} else {
					if hrec.Day730 > 0 {
						(*hrec).Day365 = hrec.Day730
					}
				}
			}
		}
	}
}
func GetEarnings(symbol string, histdate time.Time) (time.Time, time.Time, float64, bool) {
	var lastEarnings time.Time
	var nextEarnings time.Time
	var surprise float64
	var IsEarnings bool = false
	var assigned bool = false

	eList, err := mylib2.GetEarningsForSymbol(symbol, "ALL")
	if err != nil {
		log.Fatal("Can't find earnings")
	}
	for i, eDate := range eList {
		if eDate.After(histdate) || eDate.Equal(histdate) {
			if i > 1 {
				nextEarnings = eDate
				lastEarnings = eList[i-1]
				_, eRec := mylib2.GetOneEarningsRec(symbol, lastEarnings)
				if eRec.EpsEstimated != 0 {
					surprise = math.Abs((eRec.Eps - eRec.EpsEstimated) / eRec.EpsEstimated)
				}
				if eDate.Equal(histdate) {
					IsEarnings = true
				}
				assigned = true
			}
			break
		}
	}
	if !assigned {
		if len(eList) > 0 {
			lastEarnings = eList[len(eList)-1]
			_, eRec := mylib2.GetOneEarningsRec(symbol, lastEarnings)
			if eRec.EpsEstimated != 0 {
				surprise = math.Abs((eRec.Eps - eRec.EpsEstimated) / eRec.EpsEstimated)
			}
		}

	}
	return lastEarnings, nextEarnings, surprise, IsEarnings
}

func CompareDateLists2(DateList []mylib2.DateRec, trendDates []time.Time) ([]mylib2.DateRec, bool) {
	var result []mylib2.DateRec
	var status bool = true

	if len(DateList) == 0 || len(trendDates) == 0 {
		status = false
	}
	//dateTMap := make(map[time.Time]bool)
	trendMap := make(map[time.Time]bool)
	for _, t := range trendDates {
		trendMap[t] = true
	}

	for _, d := range DateList {
		_, ok := trendMap[d.Ddate]
		if !ok {
			result = append(result, d)
		}
	}
	return result, status
}

// CompareDateLists looks from the end of the list to find the first matching date.. returns the different
func CompareDateLists(DateList []mylib2.DateRec, trendDates []time.Time) ([]mylib2.DateRec, error) {
	var result []mylib2.DateRec
	//sort in latest first
	if len(DateList) == 0 {
		newErr := errors.New("empty date list")
		return result, newErr
	}
	if len(trendDates) == 0 {
		return DateList, nil
	}
	sort.Slice(DateList, func(i, j int) bool {
		return DateList[i].Ddate.After(DateList[j].Ddate)
	})
	sort.Slice(trendDates, func(i, j int) bool {
		return trendDates[i].After(trendDates[j])
	})
	var trendInd int = 0
	//fmt.Printf("%v,%v\n", trendDates[0], trendDates[trendInd])
	for _, d := range DateList {
		//fmt.Printf("d = %v, trend = %v\n", d.Ddate, trendDates[trendInd])
		if !mylib2.DateEqual(d.Ddate, trendDates[trendInd]) {
			result = append(result, d)
		} else {
			break
		}
	}
	return result, nil
}

func AddExpectedMoves(symbol string) {
	fmt.Printf("Calculating Expected Moves for %v\n", symbol)
	filter := bson.D{{Key: "underlying", Value: symbol}}
	stockHist, hstatus := mylib2.GetStockHistoryData(filter)
	if !hstatus {
		fmt.Printf("no stock history for %v. skipping symbol\n", symbol)
		return
	}
	hmap := make(map[time.Time]mylib2.StockHistory)

	for _, day := range stockHist {
		hmap[day.Datadate] = day
	}
	for _, day := range stockHist {
		if day.NextEarningsDate.After(day.Datadate) {
			ok, diff := mylib2.CalcDiffBeteenStockHistoryDates(day.Datadate, day.NextEarningsDate, stockHist)
			if ok && (diff > 0 && diff <= 2) {
				eDay, ok := hmap[day.NextEarningsDate]
				if ok {
					good, expMove := mylib2.CalcExpectedMove(eDay)
					if good {
						filter := bson.D{{Key: "underlying", Value: symbol}, {Key: "datadate", Value: day.Datadate}}
						update := bson.D{{Key: "$set", Value: bson.D{{Key: "expectedmove", Value: expMove}}}}
						matched, updated := mylib2.UpdateOne("StockHistory", filter, update)
						if matched != 1 && updated != 1 {
							fmt.Printf("Issue Updating document %v %v\n", filter, update)
						}
					}
				}
			}
		}
	}
}

func AddExpectedMovePercentiles(symbol string) {

	fmt.Printf("Calculating Expected Move Percentiles for %v\n", symbol)
	filter := bson.D{{Key: "underlying", Value: symbol}, {Key: "expectedmove", Value: bson.M{"$gt": 0}}}
	stockHist, hstatus := mylib2.GetStockHistoryData(filter)
	if !hstatus {
		fmt.Printf("no stock history for %v. skipping symbol\n", symbol)
		return
	}
	moves := mylib2.GetFloats("expectedmove", stockHist)

	for _, day := range stockHist {
		percentile := MyPercentile(moves, day.ExpectedMove)
		filter := bson.D{{Key: "underlying", Value: symbol}, {Key: "datadate", Value: day.Datadate}}
		update := bson.D{{Key: "$set", Value: bson.D{{Key: "expectedmovepercentile", Value: percentile}}}}
		matched, updated := mylib2.UpdateOne("StockHistory", filter, update)
		if matched != 1 && updated != 1 {
			fmt.Printf("Issue Updating document %v %v\n", filter, update)
		}
	}
}

/*
	Percentile Logic:
	For a given duration (aka Weekly, 30 day, 365 day, etc)
	1. Take 250 days of trend data for that duration starting with day 250
	2. Get floats out
	3. Calc percentile
	4. update record
*/

func AddIVpercentiles(symbol string) {

	var slice []mylib2.StockHistory
	fmt.Printf("Calculating IV Percentiles for %v\n", symbol)
	filter := bson.D{{Key: "underlying", Value: symbol}}
	stockHist, hstatus := mylib2.GetStockHistoryData(filter)
	if !hstatus {
		fmt.Printf("no stock history for %v. skipping symbol\n", symbol)
		return
	}

	if len(stockHist) >= TDAYSANNUALY {
		//slice = stockHist[len(stockHist)-TDAYSANNUALY:]
		for i, h := range stockHist {
			if i >= TDAYSANNUALY {
				if i == TDAYSANNUALY {
					slice = stockHist[:i+1]
				} else {
					sliceStart := i - TDAYSANNUALY
					slice = stockHist[sliceStart : i+1]
				}
				floats30 := mylib2.GetFloats("day30", slice)
				h.IVPercentile30 = MyPercentile(floats30, h.Day30)
				h.Day30VoIV = mylib2.VoIV(floats30, TDAYSANNUALY)
				floats60 := mylib2.GetFloats("day60", slice)
				h.IVPercentile60 = MyPercentile(floats60, h.Day60)
				h.Day60VoIV = mylib2.VoIV(floats60, TDAYSANNUALY)
				floats90 := mylib2.GetFloats("day90", slice)
				h.IVPercentile90 = MyPercentile(floats90, h.Day90)
				h.Day90VoIV = mylib2.VoIV(floats90, TDAYSANNUALY)
				floats120 := mylib2.GetFloats("day120", slice)
				h.IVPercentile120 = MyPercentile(floats120, h.Day120)
				//if h.IVPercentile120 == 0 {
				//	fmt.Printf("STOP FOR DEBUG")
				//}
				h.Day120VoIV = mylib2.VoIV(floats120, TDAYSANNUALY)
				floats180 := mylib2.GetFloats("day180", slice)
				h.IVPercentile180 = MyPercentile(floats180, h.Day180)
				h.Day180VoIV = mylib2.VoIV(floats180, TDAYSANNUALY)
				floats240 := mylib2.GetFloats("day240", slice)
				h.IVPercentile240 = MyPercentile(floats240, h.Day240)
				h.Day240VoIV = mylib2.VoIV(floats240, TDAYSANNUALY)
				floats365 := mylib2.GetFloats("day365", slice)
				h.IVPercentile365 = MyPercentile(floats365, h.Day365)
				h.Day365VoIV = mylib2.VoIV(floats365, TDAYSANNUALY)
				floats480 := mylib2.GetFloats("day480", slice)
				h.IVPercentile480 = MyPercentile(floats480, h.Day480)
				h.Day480VoIV = mylib2.VoIV(floats480, TDAYSANNUALY)
				floats600 := mylib2.GetFloats("day600", slice)
				h.IVPercentile600 = MyPercentile(floats600, h.Day600)
				h.Day600VoIV = mylib2.VoIV(floats600, TDAYSANNUALY)
				floats730 := mylib2.GetFloats("day730", slice)
				h.IVPercentile730 = MyPercentile(floats730, h.Day730)
				h.Day730VoIV = mylib2.VoIV(floats730, TDAYSANNUALY)
				floats850 := mylib2.GetFloats("day850", slice)
				h.IVPercentile850 = MyPercentile(floats850, h.Day850)
				h.Day850VoIV = mylib2.VoIV(floats850, TDAYSANNUALY)
				// Calc IV percentile changes
				ud1 := CalcUpriceChange2(slice, len(slice)-1, -1)
				ud5 := CalcUpriceChange2(slice, len(slice)-1, -5)
				ud10 := CalcUpriceChange2(slice, len(slice)-1, -10)
				ud30 := CalcUpriceChange2(slice, len(slice)-1, -TDAYSMONTHLY)
				ud90 := CalcUpriceChange2(slice, len(slice)-1, -TDAYSQUARTERLY)
				ud365 := CalcUpriceChange2(slice, len(slice)-1, -TDAYSANNUALY)

				// Calc HistVol
				hvol, maxUp, maxDown := CalcHistVolForPeriod(slice, TDAYSANNUALY)
				//update rec
				filter := bson.D{{Key: "underlying", Value: h.Underlying}, {Key: "datadate", Value: h.Datadate}}
				update := bson.D{{Key: "$set", Value: bson.D{{Key: "ivpercentile30", Value: h.IVPercentile30},
					{Key: "ivpercentile60", Value: h.IVPercentile60},
					{Key: "ivpercentile90", Value: h.IVPercentile90},
					{Key: "ivpercentile120", Value: h.IVPercentile120},
					{Key: "ivpercentile180", Value: h.IVPercentile180},
					{Key: "ivpercentile240", Value: h.IVPercentile240},
					{Key: "ivpercentile365", Value: h.IVPercentile365},
					{Key: "ivpercentile480", Value: h.IVPercentile480},
					{Key: "ivpercentile600", Value: h.IVPercentile600},
					{Key: "ivpercentile730", Value: h.IVPercentile730},
					{Key: "ivpercentile850", Value: h.IVPercentile850},
					{Key: "day30voiv", Value: h.Day30VoIV},
					{Key: "day60voiv", Value: h.Day60VoIV},
					{Key: "day90voiv", Value: h.Day90VoIV},
					{Key: "day120voiv", Value: h.Day120VoIV},
					{Key: "day180voiv", Value: h.Day180VoIV},
					{Key: "day240voiv", Value: h.Day240VoIV},
					{Key: "day365voiv", Value: h.Day365VoIV},
					{Key: "day480voiv", Value: h.Day480VoIV},
					{Key: "day600voiv", Value: h.Day600VoIV},
					{Key: "day730voiv", Value: h.Day730VoIV},
					{Key: "day850voiv", Value: h.Day850VoIV},
					{Key: "upricechange1", Value: ud1},
					{Key: "upricechange5", Value: ud5},
					{Key: "upricechange10", Value: ud10},
					{Key: "upricechange30", Value: ud30},
					{Key: "upricechange90", Value: ud90},
					{Key: "upricechange365", Value: ud365},
					{Key: "histvol", Value: hvol},
					{Key: "maxup", Value: maxUp},
					{Key: "maxdown", Value: maxDown}}}}
				matched, updated := mylib2.UpdateOne("StockHistory", filter, update)
				if matched != 1 {
					fmt.Printf("Issue Updating document %v %v\n", h, updated)
				}

			}
		}

	}
}

func CalcUpriceChange2(hist []mylib2.StockHistory, currRecIdx int, offset int) float64 {
	var change float64

	targetRecIdx := currRecIdx + offset
	if targetRecIdx >= 0 {
		if hist[targetRecIdx].UnderlyingPrice > 0 {
			change = (hist[currRecIdx].UnderlyingPrice - hist[targetRecIdx].UnderlyingPrice) / hist[targetRecIdx].UnderlyingPrice
		}
	}

	return change
}

// functions to calculate IV percentiles
func GetFloats(list []mylib2.StockHistory, duration string) []float64 {
	var res []float64

	for _, l := range list {
		switch duration {
		case "day5":
			if l.Day30 > 0 {
				res = append(res, l.Day5)
			}
		case "day10":
			if l.Day30 > 0 {
				res = append(res, l.Day10)
			}
		case "day30":
			if l.Day30 > 0 {
				res = append(res, l.Day30)
			}

		case "day60":
			if l.Day60 > 0 {
				res = append(res, l.Day60)
			}

		case "day90":
			if l.Day90 > 0 {
				res = append(res, l.Day90)
			}

		case "day120":
			if l.Day120 > 0 {
				res = append(res, l.Day120)
			}

		case "day180":
			if l.Day180 > 0 {
				res = append(res, l.Day180)
			}

		case "day240":
			if l.Day240 > 0 {
				res = append(res, l.Day240)
			}

		case "day365":
			if l.Day365 > 0 {
				res = append(res, l.Day365)
			}

		default:
			if l.Day365 > 0 {
				res = append(res, l.Day365)
			}
		}
	}
	return res
}

// finds the ranking of val in the list of floats (data)
func MyPercentile(data []float64, val float64) float64 {
	var below int = 0
	var size int
	var i int
	var percentile float64

	size = len(data)
	for i < size-1 {
		if data[i] < val {
			below++
		}
		i++
	}
	percentile = float64(below) / float64(size)

	return percentile
}

func CalcHistVolForPeriod(slice []mylib2.StockHistory, period int) (float64, float64, float64) {
	var histVol float64
	var maxUp float64
	var maxDown float64
	var last float64 = 0
	var returns []float64
	var sum float64
	var sumofdeviations float64
	var mean float64
	var sigma float64
	var periods int
	var startprice float64
	var prices []float64

	for _, p := range slice {
		prices = append(prices, p.UnderlyingPrice)
	}
	if len(prices) > 1 {

		for i, p := range prices {
			if i == 0 {
				last = p
				startprice = p
			} else {
				ret := math.Log(p / last)
				returns = append(returns, ret)
				last = p
				// maxup/down logi
				if p > startprice && p > maxUp {
					maxUp = p
				}
				if p < startprice && (p < maxDown || maxDown == 0) {
					if maxDown == -1 {
						fmt.Printf("STOP")
					}
					maxDown = p
				}
			}
		}
		maxUp = (maxUp - startprice) / startprice
		maxDown = (maxDown - startprice) / startprice

		for _, r := range returns {
			sum = sum + r
		}
		periods = len(returns)
		mean = sum / float64(periods)
		for _, r := range returns {
			sumofdeviations = sumofdeviations + math.Pow((r-mean), 2)
		}
		sigma = sumofdeviations / (float64(periods) - 1)
		histVol = math.Sqrt(float64(period)) * math.Sqrt(sigma)
	}
	return histVol, maxUp, maxDown
}
func Percentile(data []float64, percent float64) (float64, error) {
	if percent < 0 || percent > 100 {
		return 0, fmt.Errorf("percentile must be between 0 and 100")
	}

	if len(data) == 0 {
		return 0, fmt.Errorf("cannot calculate percentile on empty data")
	}

	// Sort the data
	sortedData := make([]float64, len(data))
	copy(sortedData, data)
	sort.Float64s(sortedData)

	// Calculate the rank
	rank := (percent / 100) * float64(len(data)-1)

	// Check for edge cases
	if rank <= 0 {
		return sortedData[0], nil
	} else if rank >= float64(len(data)-1) {
		return sortedData[len(data)-1], nil
	}

	// Calculate the percentile using linear interpolation
	lowerIndex := int(math.Floor(rank))
	upperIndex := lowerIndex + 1
	weight := rank - float64(lowerIndex)

	return sortedData[lowerIndex]*(1-weight) + sortedData[upperIndex]*weight, nil
}

/*
for _, underlying := range SymbolList {
		History := VolTrend(underlying, lookback)

		_, err = mylib2.InsertStockHistoryRecsBulk(History)
		if err != nil {
			fmt.Println("Trouble inserting stock history data")
			fmt.Println(err)
			return
		}

		//Next lets add ratings
		AddRatings(underlying)
		AddIVpercentiles(underlying)
		fmt.Printf("Finished %v %v\n", underlying, time.Now())
	}
*/
