package storage

type Storage interface {
	CreateApplication(app Etl) error
	GetEtlApplication(id int64) (*Etl, error)
	ListEtlApplication() (EtlList, error)
}
