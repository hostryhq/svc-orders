package api

import (
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/hostryhq/svc-orders/internal/gen"
	"github.com/hostryhq/svc-orders/service"
)

func deref[T any](p *T) (v T) {
	if p != nil {
		v = *p
	}
	return v
}

func date(d *openapi_types.Date) string {
	if d == nil {
		return ""
	}
	return d.Format("2006-01-02")
}

func (h *Handler) GetOpenCheckCount(w http.ResponseWriter, r *http.Request, outletID gen.Id) {
	v, err := h.s.OpenCheckCount(r.Context(), outletID)
	respond(w, r, v, err)
}

func (h *Handler) ListChecks(w http.ResponseWriter, r *http.Request, p gen.ListChecksParams) {
	v, err := h.s.ListChecks(r.Context(), service.CheckFilter{Status: string(deref(p.Status)), OrderType: string(deref(p.OrderType)),
		SeatingID: deref(p.SeatingId), Cursor: deref(p.Cursor), Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetCheck(w http.ResponseWriter, r *http.Request, id gen.Id) {
	v, err := h.s.GetCheck(r.Context(), id)
	respond(w, r, v, err)
}

func (h *Handler) ListOrderExceptions(w http.ResponseWriter, r *http.Request, p gen.ListOrderExceptionsParams) {
	v, err := h.s.ListExceptions(r.Context(), service.ExceptionFilter{Status: string(deref(p.Status)), Kind: deref(p.Kind),
		Cursor: deref(p.Cursor), Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetOrderException(w http.ResponseWriter, r *http.Request, id gen.Id) {
	v, err := h.s.GetException(r.Context(), id)
	respond(w, r, v, err)
}

func (h *Handler) GetOrdersProjectionDigest(w http.ResponseWriter, r *http.Request, p gen.GetOrdersProjectionDigestParams) {
	v, err := h.s.ProjectionDigest(r.Context(), deref(p.XOutletId), date(p.BusinessDate))
	respond(w, r, v, err)
}

func (h *Handler) GetCancellationReport(w http.ResponseWriter, r *http.Request, p gen.GetCancellationReportParams) {
	v, err := h.s.CancellationReport(r.Context(), service.ReportFilter{From: date(p.From), To: date(p.To), Kind: string(deref(p.Kind)),
		Cursor: deref(p.Cursor), Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetKitchenTimeReport(w http.ResponseWriter, r *http.Request, p gen.GetKitchenTimeReportParams) {
	v, err := h.s.KitchenTimes(r.Context(), service.ReportFilter{From: date(p.From), To: date(p.To), Cursor: deref(p.Cursor),
		Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetKotLog(w http.ResponseWriter, r *http.Request, p gen.GetKotLogParams) {
	v, err := h.s.KotLog(r.Context(), service.ReportFilter{From: date(p.From), To: date(p.To), Cursor: deref(p.Cursor),
		Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetOpenChecksReport(w http.ResponseWriter, r *http.Request, p gen.GetOpenChecksReportParams) {
	v, err := h.s.OpenChecks(r.Context(), service.ReportFilter{From: date(p.From), To: date(p.To), Cursor: deref(p.Cursor),
		Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}

func (h *Handler) GetPresentLog(w http.ResponseWriter, r *http.Request, p gen.GetPresentLogParams) {
	v, err := h.s.PresentLog(r.Context(), service.ReportFilter{From: date(p.From), To: date(p.To), Cursor: deref(p.Cursor),
		Limit: int(deref(p.Limit)), OutletHeader: deref(p.XOutletId)})
	respond(w, r, v, err)
}
