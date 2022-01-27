package main

import "time"

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

// TODO: Check if mongo or dynamo. Implement db adapter maybe?
func buildCompany(record []string) Company {
	incDateTimestamp, err := time.Parse("02/01/2006", record[14])
	if err != nil {
		incDateTimestamp = time.Now() // Default to today's date
	}
	company := Company{
		CompanyName:                record[0],
		CompanyNumber:              record[1],
		CareOf:                     record[2],
		POBox:                      record[3],
		AddressLine1:               record[4],
		AddressLine2:               record[5],
		PostTown:                   record[6],
		County:                     record[7],
		Country:                    record[8],
		PostCode:                   record[9],
		CompanyCategory:            record[10],
		CompanyStatus:              record[11],
		CountryOfOrigin:            record[12],
		DissolutionDate:            record[13],
		IncorporationDate:          record[14],
		IncorporationDateTimestamp: int(incDateTimestamp.Unix()),
		AccountingRefDay:           record[15],
		AccountingRefMonth:         record[16],
		NextDueDate:                record[17],
		LastMadeUpDate:             record[18],
		AccountsCategory:           record[19],
		ReturnsNextDueDate:         record[20],
		ReturnsLastMadeUpDate:      record[21],
		NumMortCharges:             record[22],
		NumMortOutstanding:         record[23],
		NumMortPartSatisfied:       record[24],
		NumMortSatisfied:           record[25],
		SICCode1:                   record[26],
		SICCode2:                   record[27],
		SICCode3:                   record[28],
		SICCode4:                   record[29],
		NumGenPartners:             record[30],
		NumLimPartners:             record[31],
		URI:                        record[32],
		ChangeOfNameDate:           record[33],
		CompanyNamePrevious:        record[34],
	}
	return company
}
