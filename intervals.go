package q

import "github.com/oov/q/qutil"

type years int

func (i years) Value() int               { return int(i) }
func (i years) Unit() qutil.IntervalUnit { return qutil.Year }

// Years creates Interval such as "INTERVAL n YEAR".
func Years(n int) Interval { return years(n) }

type months int

func (i months) Value() int               { return int(i) }
func (i months) Unit() qutil.IntervalUnit { return qutil.Month }

// Months creates Interval such as "INTERVAL n MONTH".
func Months(n int) Interval { return months(n) }

type days int

func (i days) Value() int               { return int(i) }
func (i days) Unit() qutil.IntervalUnit { return qutil.Day }

// Days creates Interval such as "INTERVAL n DAY".
func Days(n int) Interval { return days(n) }

type hours int

func (i hours) Value() int               { return int(i) }
func (i hours) Unit() qutil.IntervalUnit { return qutil.Hour }

// Hours creates Interval such as "INTERVAL n HOUR".
func Hours(n int) Interval { return hours(n) }

type minutes int

func (i minutes) Value() int               { return int(i) }
func (i minutes) Unit() qutil.IntervalUnit { return qutil.Minute }

// Minutes creates Interval such as "INTERVAL n MINUTE".
func Minutes(n int) Interval { return minutes(n) }

type seconds int

func (i seconds) Value() int               { return int(i) }
func (i seconds) Unit() qutil.IntervalUnit { return qutil.Second }

// Seconds creates Interval such as "INTERVAL n SECOND".
func Seconds(n int) Interval { return seconds(n) }
