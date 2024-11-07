package webservice

import (
	"github.com/G-Research/yunikorn-core/pkg/scheduler/objects"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
)

func GetApplicationDAO(app *objects.Application, summary *objects.ApplicationSummary) *dao.ApplicationDAOInfo {
	return getApplicationDAO(app, summary)
}
