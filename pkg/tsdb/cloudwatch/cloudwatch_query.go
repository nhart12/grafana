package cloudwatch

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type cloudWatchQuery struct {
	RefId                   string
	Region                  string
	Id                      string
	Namespace               string
	MetricName              string
	Stats                   string
	Expression              string
	ReturnData              bool
	Dimensions              map[string][]string
	UsedPeriod              int
	RequestedPeriod         int
	AutoPeriod              bool
	Alias                   string
	MatchExact              bool
	UsedExpression          string
	RequestExceededMaxLimit bool
}

func (q *cloudWatchQuery) isMathExpression() bool {
	return q.Expression != "" && !q.isUserDefinedSearchExpression()
}

func (q *cloudWatchQuery) isSearchExpression() bool {
	return q.isUserDefinedSearchExpression() || q.isInferredSearchExpression()
}

func (q *cloudWatchQuery) isUserDefinedSearchExpression() bool {
	return strings.Contains(q.Expression, "SEARCH(")
}

func (q *cloudWatchQuery) isInferredSearchExpression() bool {
	if len(q.Dimensions) == 0 {
		return !q.MatchExact
	}

	if !q.MatchExact {
		return true
	}

	for _, values := range q.Dimensions {
		if len(values) > 1 {
			return true
		}
		for _, v := range values {
			if v == "*" {
				return true
			}
		}
	}
	return false
}

func (q *cloudWatchQuery) isMetricStat() bool {
	return !q.isSearchExpression() && !q.isMathExpression()
}

func (q *cloudWatchQuery) setUsedPeriod(startTime time.Time, endTime time.Time, batchContainsWildcard bool, noOfQueries int) {
	if q.RequestedPeriod == 0 {
		if batchContainsWildcard {
			q.UsedPeriod = 60
		} else {
			delta := endTime.Sub(startTime)
			hours := math.Ceil(delta.Hours())
			ms := delta.Milliseconds()
			datapointsPerSecond := 90000
			if hours <= 3 {
				datapointsPerSecond = 180000
			}
			test := math.Ceil(float64(delta.Milliseconds()) / 1000.0 / 60.0 / datapointsPerSecond / noOfQueries)
			q.UsedPeriod = int(test * 60)
			fmt.Println(hours)
			// q.UsedPeriod, _ = int()

		}
	} else {
		q.UsedPeriod = q.RequestedPeriod
	}
}
