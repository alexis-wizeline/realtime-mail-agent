package processors

import "context"

type MockProcessFn func(context.Context, Job) error

type MockProcessor struct {
	exec MockProcessFn
}

func NewMockProcess(p MockProcessFn) Processor {
	return MockProcessor{
		exec: p,
	}
}

func (m MockProcessor) Process(ctx context.Context, j Job) error {
	if m.exec != nil {
		return m.exec(ctx, j)
	}
	return nil
}
