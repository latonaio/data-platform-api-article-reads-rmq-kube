package dpfm_api_output_formatter

import (
	"data-platform-api-article-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*[]Header, error) {
	defer rows.Close()
	header := make([]Header, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Header{}

		err := rows.Scan(
			&pm.Article,
			&pm.ArticleType,
			&pm.ArticleOwner,
			&pm.ArticleOwnerBusinessPartnerRole,
			&pm.PersonResponsible,
			&pm.ValidityStartDate,
			&pm.ValidityStartTime,
			&pm.ValidityEndDate,
			&pm.ValidityEndTime,
			&pm.Description,
			&pm.LongText,
			&pm.Introduction,
			&pm.Site,
			&pm.Shop,
			&pm.Project,
			&pm.WBSElement,
			&pm.Tag1,
			&pm.Tag2,
			&pm.Tag3,
			&pm.Tag4,
			&pm.DistributionProfile,
			&pm.QuestionnaireType,
			&pm.QuestionnaireTemplate,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
			&pm.CreateUser,
			&pm.LastChangeUser,
			&pm.IsReleased,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

		data := pm
		header = append(header, Header{
			Article:							data.Article,
			ArticleType:						data.ArticleType,
			ArticleOwner:						data.ArticleOwner,
			ArticleOwnerBusinessPartnerRole:	data.ArticleOwnerBusinessPartnerRole,
			PersonResponsible:					data.PersonResponsible,
			ValidityStartDate:					data.ValidityStartDate,
			ValidityStartTime:					data.ValidityStartTime,
			ValidityEndDate:					data.ValidityEndDate,
			ValidityEndTime:					data.ValidityEndTime,
			Description:						data.Description,
			LongText:							data.LongText,
			Introduction:						data.Introduction,
			Site:								data.Site,
			Shop:								data.Shop,
			Project:							data.Project,
			WBSElement:							data.WBSElement,
			Tag1:								data.Tag1,
			Tag2:								data.Tag2,
			Tag3:								data.Tag3,
			Tag4:								data.Tag4,
			DistributionProfile:				data.DistributionProfile,
			QuestionnaireType:					data.QuestionnaireType,
			QuestionnaireTemplate:				data.QuestionnaireTemplate,
			CreationDate:						data.CreationDate,
			CreationTime:						data.CreationTime,
			LastChangeDate:						data.LastChangeDate,
			LastChangeTime:						data.LastChangeTime,
			CreateUser:							data.CreateUser,
			LastChangeUser:						data.LastChangeUser,
			IsReleased:							data.IsReleased,
			IsMarkedForDeletion:				data.IsMarkedForDeletion,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &header, nil
	}

	return &header, nil
}

func ConvertToPartner(rows *sql.Rows) (*[]Partner, error) {
	defer rows.Close()
	partner := make([]Partner, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Partner{}

		err := rows.Scan(
			&pm.Article,
			&pm.PartnerFunction,
			&pm.BusinessPartner,
			&pm.BusinessPartnerFullName,
			&pm.BusinessPartnerName,
			&pm.Organization,
			&pm.Country,
			&pm.Language,
			&pm.Currency,
			&pm.ExternalDocumentID,
			&pm.AddressID,
			&pm.EmailAddress,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &partner, err
		}

		data := pm
		partner = append(partner, Partner{
			Article:                   data.Article,
			PartnerFunction:         data.PartnerFunction,
			BusinessPartner:         data.BusinessPartner,
			BusinessPartnerFullName: data.BusinessPartnerFullName,
			BusinessPartnerName:     data.BusinessPartnerName,
			Organization:            data.Organization,
			Country:                 data.Country,
			Language:                data.Language,
			Currency:                data.Currency,
			ExternalDocumentID:      data.ExternalDocumentID,
			AddressID:               data.AddressID,
			EmailAddress:            data.EmailAddress,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &partner, nil
	}

	return &partner, nil
}

func ConvertToAddress(rows *sql.Rows) (*[]Address, error) {
	defer rows.Close()
	address := make([]Address, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Address{}

		err := rows.Scan(
			&pm.Article,
			&pm.AddressID,
			&pm.PostalCode,
			&pm.LocalSubRegion,
			&pm.LocalRegion,
			&pm.Country,
			&pm.GlobalRegion,
			&pm.TimeZone,
			&pm.District,
			&pm.StreetName,
			&pm.CityName,
			&pm.Building,
			&pm.Floor,
			&pm.Room,
			&pm.XCoordinate,
			&pm.YCoordinate,
			&pm.ZCoordinate,
			&pm.Site,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &address, err
		}

		data := pm
		address = append(address, Address{
			Article:       	data.Article,
			AddressID:   	data.AddressID,
			PostalCode:  	data.PostalCode,
			LocalSubRegion: data.LocalSubRegion,
			LocalRegion: 	data.LocalRegion,
			Country:     	data.Country,
			GlobalRegion: 	data.GlobalRegion,
			TimeZone:	 	data.TimeZone,
			District:    	data.District,
			StreetName:  	data.StreetName,
			CityName:    	data.CityName,
			Building:    	data.Building,
			Floor:       	data.Floor,
			Room:        	data.Room,
			XCoordinate: 	data.XCoordinate,
			YCoordinate: 	data.YCoordinate,
			ZCoordinate: 	data.ZCoordinate,
			Site:		 	data.Site,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &address, nil
	}

	return &address, nil
}

func ConvertToCounter(rows *sql.Rows) (*[]Counter, error) {
	defer rows.Close()
	counter := make([]Counter, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Counter{}

		err := rows.Scan(
			&pm.Article,
			&pm.NumberOfLikes,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &counter, err
		}

		data := pm
		counter = append(counter, Counter{
			Article:				data.Article,
			NumberOfLikes:			data.NumberOfLikes,
			CreationDate:			data.CreationDate,
			CreationTime:			data.CreationTime,
			LastChangeDate:			data.LastChangeDate,
			LastChangeTime:			data.LastChangeTime,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &counter, nil
	}

	return &counter, nil
}

func ConvertToLike(rows *sql.Rows) (*[]Like, error) {
	defer rows.Close()
	like := make([]Like, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Like{}

		err := rows.Scan(
			&pm.Article,
			&pm.BusinessPartner,
			&pm.Like,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &like, err
		}

		data := pm
		like = append(like, Like{
			Article:				data.Article,
			BusinessPartner:		data.BusinessPartner,
			Like:					data.Like,
			CreationDate:			data.CreationDate,
			CreationTime:			data.CreationTime,
			LastChangeDate:			data.LastChangeDate,
			LastChangeTime:			data.LastChangeTime,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &like, nil
	}

	return &like, nil
}
