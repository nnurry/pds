package metadata

import (
	"errors"

	"github.com/nnurry/pds/db"
	"github.com/nnurry/pds/metadata"
)

type filterService struct {
	repo *filterRepo
}

func NewFilterService() *filterService {
	return &filterService{
		repo: NewFilterRepo(),
	}
}

func (s *filterService) CreateFilter(
	filterType string, key string,
	maxCardinality uint, maxFp float64,
) (metadata.Filter, error) {

	var filter metadata.Filter
	var err error

	switch filterType {
	case "STD_BLOOM":
		filter = metadata.NewStdBloom(
			key, filterType,
			maxCardinality, maxFp,
		)
	case "REDIS_BLOOM":
		filter = metadata.NewRedisBloom(
			key, filterType,
			maxCardinality, maxFp,
		)
	default:
		err = errors.New("not implemented: " + filterType)
		return nil, err
	}

	tx, err := db.PostgresClient().Begin()

	if err != nil {
		return nil, err
	}

	err = s.repo.InsertFilter(tx, true, InsertFilterPayload{
		Type:           filterType,
		Key:            key,
		MaxCardinality: maxCardinality,
		MaxFp:          maxFp,
		HashFuncNum:    filter.HashFuncNum(),
		HashFuncType:   filter.HashFuncType(),
	})

	if err != nil {
		return nil, err
	}

	return filter, err
}

type cardinalService struct {
	repo *cardinalRepo
}

func NewCardinalService() *cardinalService {
	return &cardinalService{
		repo: NewCardinalRepo(),
	}
}

func (s *cardinalService) CreateCardinal(cardinalType string, key string) (metadata.Cardinal, error) {

	var cardinal metadata.Cardinal
	var err error

	switch cardinalType {
	case "STD_HLL":
		cardinal = metadata.NewStdHLL(14, false, key)
	case "REDIS_BLOOM":
		cardinal = metadata.NewRedisHLL(key)
	default:
		err = errors.New("not implemented: " + cardinalType)
		return nil, err
	}

	tx, err := db.PostgresClient().Begin()

	if err != nil {
		return nil, err
	}

	blob := cardinal.Serialize()

	err = s.repo.InsertCardinal(tx, true, InsertCardinalPayload{
		Type: cardinalType,
		Key:  key,
		Blob: blob,
	})

	if err != nil {
		return nil, err
	}

	return cardinal, err
}
