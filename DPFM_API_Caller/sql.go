package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-article-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-article-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var header *[]dpfm_api_output_formatter.Header
	var address *[]dpfm_api_output_formatter.Address
	var partner *[]dpfm_api_output_formatter.Partner
	var counter *[]dpfm_api_output_formatter.Counter
	var like *[]dpfm_api_output_formatter.Like

	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				header = c.Header(mtx, input, output, errs, log)
			}()
		case "Headers":
			func() {
				header = c.Headers(mtx, input, output, errs, log)
			}()
		case "HeadersByArticles":
			func() {
				header = c.HeadersByArticles(mtx, input, output, errs, log)
			}()
		case "HeadersBySite":
			func() {
				header = c.HeadersBySite(mtx, input, output, errs, log)
			}()
		case "HeadersByArticleOwner":
			func() {
				header = c.HeadersByArticleOwner(mtx, input, output, errs, log)
			}()
		case "Partner":
			func() {
				partner = c.Partner(mtx, input, output, errs, log)
			}()
		case "Partners":
			func() {
				partner = c.Partners(mtx, input, output, errs, log)
			}()
		case "Address":
			func() {
				address = c.Address(mtx, input, output, errs, log)
			}()
		case "Addresses":
			func() {
				address = c.Addresses(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalSubRegion":
			func() {
				address = c.AddressesByLocalSubRegion(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalSubRegions":
			func() {
				address = c.AddressesByLocalSubRegions(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalRegion":
			func() {
				address = c.AddressesByLocalRegion(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalRegions":
			func() {
				address = c.AddressesByLocalRegions(mtx, input, output, errs, log)
			}()
		case "Counter":
			func() {
				counter = c.Counter(mtx, input, output, errs, log)
			}()
		case "CountersByArticles":
			func() {
				counter = c.CountersByArticles(mtx, input, output, errs, log)
			}()
		case "Like":
			func() {
				like = c.Like(mtx, input, output, errs, log)
			}()
		default:
		}
		if len(*errs) != 0 {
			break
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Header:                header,
		Partner:               partner,
		Address:               address,
		Counter:               counter,
		Like:                  like,
	}

	return data
}

func (c *DPFMAPICaller) Header(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Article = %d", input.Header.Article)

	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Headers(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := "WHERE 1 = 1"
	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersByArticles(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	log.Info("HeadersByArticles")
	in := ""

	for iHeader, vHeader := range input.Headers {
		article := vHeader.Article
		if iHeader == 0 {
			in = fmt.Sprintf(
				"( '%d' )",
				article,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%d' )",
			in,
			article,
		)
	}

	where := fmt.Sprintf(" WHERE ( Article ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersBySite(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {

	where := fmt.Sprintf("WHERE header.Site = %d", input.Header.Site)

	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersByArticleOwner(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {

	where := fmt.Sprintf("WHERE header.ArticleOwner = %d", input.Header.ArticleOwner)

	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Partner(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Partner {
	var args []interface{}
	article := input.Header.Article
	partner := input.Header.Partner

	cnt := 0
	for _, v := range partner {
		args = append(args, article, v.PartnerFunction, v.BusinessPartner)
		cnt++
	}
	repeat := strings.Repeat("(?,?,?),", cnt-1) + "(?,?,?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_partner_data
		WHERE (Article, PartnerFunction, BusinessPartner) IN ( `+repeat+` ) 
		ORDER BY Article ASC, PartnerFunction ASC, BusinessPartner ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPartner(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Partners(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Partner {
	var args []interface{}
	article := input.Header.Article
	partner := input.Header.Partner

	cnt := 0
	for _, _ = range partner {
		args = append(args, article)
		cnt++
	}
	repeat := strings.Repeat("(?),", cnt-1) + "(?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_partner_data
		WHERE (Article) IN ( `+repeat+` ) 
		ORDER BY Article ASC, PartnerFunction ASC, BusinessPartner ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPartner(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Address(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	var args []interface{}
	address := input.Header.Address

	cnt := 0
	for _, v := range address {
		args = append(args, v.Article, v.Article)
		cnt++
	}
	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data
		WHERE (Article, AddressID) IN ( `+repeat+` ) 
		ORDER BY Article ASC, AddressID ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Addresses(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	var args []interface{}
	article := input.Header.Article
	address := input.Header.Address

	cnt := 0
	for _, _ = range address {
		args = append(args, article)
		cnt++
	}
	repeat := strings.Repeat("(?),", cnt-1) + "(?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data
		WHERE (Article) IN ( `+repeat+` ) 
		ORDER BY Article ASC, AddressID ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalSubRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	where := "WHERE 1 = 1"

	if input.Header.Address[0].LocalSubRegion != nil {
		where = fmt.Sprintf("WHERE address.LocalSubRegion = \"%s\"", *input.Header.Address[0].LocalSubRegion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data AS address
		` + where + ` ORDER BY address.LocalSubRegion ASC, address.Article ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalSubRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {

	log.Info("AddressesByLocalSubRegions")
	in := ""

	for iAddress, vAddress := range input.Header.Address {
		localSubRegion := vAddress.LocalSubRegion
		if iAddress == 0 {
			in = fmt.Sprintf(
				"( '%s' )",
				localSubRegion,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%s' )",
			in,
			localSubRegion,
		)
	}

	where := fmt.Sprintf(" WHERE ( LocalSubRegion ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data AS address
		` + where + ` ORDER BY address.LocalSubRegion ASC, address.Article ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	where := "WHERE 1 = 1"

	if input.Header.Address[0].LocalRegion != nil {
		where = fmt.Sprintf("WHERE address.LocalRegion = \"%s\"", *input.Header.Address[0].LocalRegion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data AS address
		` + where + ` ORDER BY address.LocalRegion ASC, address.Article ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {

	log.Info("AddressesByLocalRegions")
	in := ""

	for iAddress, vAddress := range input.Header.Address {
		localRegion := vAddress.LocalRegion
		if iAddress == 0 {
			in = fmt.Sprintf(
				"( '%s' )",
				localRegion,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%s' )",
			in,
			localRegion,
		)
	}

	where := fmt.Sprintf(" WHERE ( LocalRegion ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_address_data AS address
		` + where + ` ORDER BY address.LocalRegion ASC, address.Article ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Counter(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Counter {
	where := fmt.Sprintf("WHERE Article = %d ", input.Header.Article)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_counter_data AS counter
		` + where + ` ORDER BY counter.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToCounter(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) CountersByArticles(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Counter {
	log.Info("CountersByArticles")
	in := ""

	for iCounter, vCounter := range input.Headers {
		article := vCounter.Article
		if iCounter == 0 {
			in = fmt.Sprintf(
				"( '%d' )",
				article,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%d' )",
			in,
			article,
		)
	}

	where := fmt.Sprintf(" WHERE ( Article ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_counter_data AS counter
		` + where + ` ORDER BY counter.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToCounter(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Like(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Like {
	where := fmt.Sprintf("WHERE Article = %d ", input.Header.Article)

	where = fmt.Sprintf("%s\nAND header.BusinessPartner = %d", where, input.Header.Partner[0].BusinessPartner)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_article_like_data AS like
		` + where + ` ORDER BY like.Article ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToLike(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
