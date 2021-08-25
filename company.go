package main

type Company struct {
	CompanyName   string
	CompanyNumber string

	CareOf string
	POBox  string

	AddressLine1 string
	AddressLine2 string
	PostTown     string
	County       string
	Country      string
	PostCode     string

	CompanyCategory            string
	CompanyStatus              string
	CountryOfOrigin            string
	DissolutionDate            string
	IncorporationDate          string
	IncorporationDateTimestamp int

	AccountingRefDay   string
	AccountingRefMonth string
	NextDueDate        string
	LastMadeUpDate     string
	AccountsCategory   string

	ReturnsNextDueDate    string
	ReturnsLastMadeUpDate string

	NumMortCharges       string
	NumMortOutstanding   string
	NumMortPartSatisfied string
	NumMortSatisfied     string

	SICCode1 string
	SICCode2 string
	SICCode3 string
	SICCode4 string

	NumGenPartners string
	NumLimPartners string

	URI string

	ChangeOfNameDate    string
	CompanyNamePrevious string
}
