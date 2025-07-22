package txingester

// Infrastructure provides access to external services for txIngester module
type Infrastructure struct {
	CCEService CCEService
}

// NewInfrastructure creates a new Infrastructure with CCE service dependency
func NewInfrastructure(cceService CCEService) *Infrastructure {
	return &Infrastructure{
		CCEService: cceService,
	}
}
