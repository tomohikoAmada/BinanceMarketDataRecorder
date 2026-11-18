#include <binance_market_data/projection/v1/numeric/decimal_error.hpp>
#include <binance_market_data/projection/v1/numeric/decimal_parse.hpp>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <simdjson.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace p = binance_market_data::projection::v1;
using simdjson::dom::element;

namespace {

constexpr int kOk = 0;
constexpr int kIgnored = 1;
constexpr int kGap = 2;
constexpr int kEmptySideMask = 4;
constexpr int kCrossedBookMask = 8;

constexpr int kErrArg = -1;
constexpr int kErrWire = -2;
constexpr int kErrDecimal = -3;
constexpr int kErrState = -4;

struct BookHandle final {
    BookHandle(p::NumericSpec spec, bool spot)
        : spec{spec},
          book{spec, spot ? p::SequencePolicyKind::Spot : p::SequencePolicyKind::UsdMPerpetual},
          is_spot{spot} {}

    p::NumericSpec spec;
    p::BookProjection book;
    bool is_spot;
    simdjson::dom::parser parser;
    std::vector<p::LevelUpdate> updates;
    std::vector<p::BookLevel> baseline_bids;
    std::vector<p::BookLevel> baseline_asks;
};

thread_local std::string tls_last_error;

void set_error(std::string msg) { tls_last_error = std::move(msg); }

const char* decimal_error_name(p::DecimalErrorCode code) noexcept {
    switch (code) {
    case p::DecimalErrorCode::Empty:
        return "Empty";
    case p::DecimalErrorCode::InvalidSyntax:
        return "InvalidSyntax";
    case p::DecimalErrorCode::SignNotAllowed:
        return "SignNotAllowed";
    case p::DecimalErrorCode::LeadingZero:
        return "LeadingZero";
    case p::DecimalErrorCode::MissingFractionDigits:
        return "MissingFractionDigits";
    case p::DecimalErrorCode::InexactScale:
        return "InexactScale";
    case p::DecimalErrorCode::Overflow:
        return "Overflow";
    case p::DecimalErrorCode::ZeroNotAllowed:
        return "ZeroNotAllowed";
    }
    return "Unknown";
}

template <typename Callback>
int parse_levels_array(element arr_elem, p::NumericSpec spec, const char* context_label,
                       Callback accept) {
    simdjson::dom::array arr;
    if (arr_elem.get_array().get(arr) != simdjson::SUCCESS) {
        set_error(std::string(context_label) + ": expected levels array");
        return kErrWire;
    }
    for (element row : arr) {
        simdjson::dom::array pair;
        if (row.get_array().get(pair) != simdjson::SUCCESS || pair.size() != 2) {
            set_error(std::string(context_label) + ": expected [price, qty] 2-tuple");
            return kErrWire;
        }
        auto it = pair.begin();
        std::string_view px_text;
        if ((*it).get_string().get(px_text) != simdjson::SUCCESS) {
            set_error(std::string(context_label) + ": expected string price");
            return kErrWire;
        }
        ++it;
        std::string_view qty_text;
        if ((*it).get_string().get(qty_text) != simdjson::SUCCESS) {
            set_error(std::string(context_label) + ": expected string quantity");
            return kErrWire;
        }

        const auto px_res = p::parse_price(px_text, spec.price_scale);
        if (const auto* err = std::get_if<p::DecimalError>(&px_res)) {
            set_error(std::string(context_label) + ": invalid/inexact price '" +
                      std::string(px_text) + "' at price_scale=" +
                      std::to_string(spec.price_scale.value()) + " (" +
                      decimal_error_name(err->code) + ")");
            return kErrDecimal;
        }
        const auto qty_res = p::parse_quantity(qty_text, spec.quantity_scale);
        if (const auto* err = std::get_if<p::DecimalError>(&qty_res)) {
            set_error(std::string(context_label) + ": invalid/inexact quantity '" +
                      std::string(qty_text) + "' at quantity_scale=" +
                      std::to_string(spec.quantity_scale.value()) + " (" +
                      decimal_error_name(err->code) + ")");
            return kErrDecimal;
        }
        accept(std::get<p::ParsedDecimal<p::PriceUnits>>(px_res).value,
               std::get<p::ParsedDecimal<p::QuantityUnits>>(qty_res).value);
    }
    return kOk;
}

} // namespace

extern "C" {

const char* bmcp_simdjson_impl() noexcept {
    static const std::string impl_name(simdjson::get_active_implementation()->name());
    return impl_name.c_str();
}

const char* bmcp_last_error() noexcept { return tls_last_error.c_str(); }

void* bmcp_book_create(int is_spot, unsigned price_scale, unsigned qty_scale) noexcept {
    try {
        const auto ps = p::DecimalScale::create(price_scale);
        const auto qs = p::DecimalScale::create(qty_scale);
        if (!ps.has_value() || !qs.has_value()) {
            set_error("decimal scale must be <= 18");
            return nullptr;
        }
        return new BookHandle(p::NumericSpec{*ps, *qs}, is_spot != 0);
    } catch (const std::exception& ex) {
        set_error(ex.what());
        return nullptr;
    }
}

void bmcp_book_free(void* raw_handle) noexcept {
    delete static_cast<BookHandle*>(raw_handle);
}

void bmcp_book_reset(void* raw_handle) noexcept {
    if (auto* h = static_cast<BookHandle*>(raw_handle)) {
        h->book.reset();
    }
}

int bmcp_book_status(void* raw_handle) noexcept {
    const auto* h = static_cast<const BookHandle*>(raw_handle);
    if (h == nullptr) {
        return kErrArg;
    }
    switch (h->book.status()) {
    case p::ProjectionStatus::AwaitingBaseline:
        return 0;
    case p::ProjectionStatus::AwaitingBridge:
        return 1;
    case p::ProjectionStatus::Synchronized:
        return 2;
    case p::ProjectionStatus::NeedsResync:
        return 3;
    }
    return kErrState;
}

int bmcp_book_last_update_id(void* raw_handle, std::uint64_t* out_uid) noexcept {
    const auto* h = static_cast<const BookHandle*>(raw_handle);
    if (h == nullptr || out_uid == nullptr) {
        return kErrArg;
    }
    const auto uid = h->book.last_update_id();
    if (!uid.has_value()) {
        return kErrState;
    }
    *out_uid = uid->value();
    return kOk;
}

int bmcp_book_install_baseline_json(void* raw_handle, const char* json_bytes,
                                    std::size_t size) noexcept {
    auto* h = static_cast<BookHandle*>(raw_handle);
    if (h == nullptr || json_bytes == nullptr) {
        set_error("null handle or baseline payload");
        return kErrArg;
    }
    try {
        element root;
        if (h->parser.parse(json_bytes, size).get(root) != simdjson::SUCCESS || !root.is_object()) {
            set_error("baseline JSON must be an object");
            return kErrWire;
        }
        std::uint64_t uid_val = 0;
        if (root["lastUpdateId"].get_uint64().get(uid_val) != simdjson::SUCCESS) {
            set_error("baseline missing uint64 lastUpdateId");
            return kErrWire;
        }
        h->baseline_bids.clear();
        h->baseline_asks.clear();
        element bids_elem;
        if (root["bids"].get(bids_elem) == simdjson::SUCCESS && !bids_elem.is_null()) {
            const int rc = parse_levels_array(
                bids_elem, h->spec, "baseline.bids",
                [&](p::PriceUnits px, p::QuantityUnits qty) {
                    h->baseline_bids.push_back({px, qty});
                });
            if (rc != kOk) {
                return rc;
            }
        }
        element asks_elem;
        if (root["asks"].get(asks_elem) == simdjson::SUCCESS && !asks_elem.is_null()) {
            const int rc = parse_levels_array(
                asks_elem, h->spec, "baseline.asks",
                [&](p::PriceUnits px, p::QuantityUnits qty) {
                    h->baseline_asks.push_back({px, qty});
                });
            if (rc != kOk) {
                return rc;
            }
        }
        const auto res = h->book.install_baseline(
            {p::UpdateId{uid_val}, h->baseline_bids, h->baseline_asks});
        if (res.disposition != p::InstallDisposition::Installed) {
            set_error("install_baseline rejected in current projection state");
            return kErrState;
        }
        return kOk;
    } catch (const std::exception& ex) {
        set_error(ex.what());
        return kErrWire;
    }
}

int bmcp_book_apply_json(void* raw_handle, const char* json_bytes, std::size_t size,
                         std::uint64_t* out_first_u, std::uint64_t* out_final_u) noexcept {
    auto* h = static_cast<BookHandle*>(raw_handle);
    if (h == nullptr || json_bytes == nullptr) {
        set_error("null handle or diff_depth payload");
        return kErrArg;
    }
    try {
        element obj;
        if (h->parser.parse(json_bytes, size).get(obj) != simdjson::SUCCESS || !obj.is_object()) {
            set_error("diff_depth JSON must be an object");
            return kErrWire;
        }
        element stream_elem;
        if (obj["stream"].get(stream_elem) == simdjson::SUCCESS) {
            element data_elem;
            if (obj["data"].get(data_elem) == simdjson::SUCCESS && data_elem.is_object()) {
                obj = data_elem;
            }
        }
        std::uint64_t first_u = 0;
        std::uint64_t final_u = 0;
        if (obj["U"].get_uint64().get(first_u) != simdjson::SUCCESS ||
            obj["u"].get_uint64().get(final_u) != simdjson::SUCCESS) {
            set_error("diff_depth missing uint64 U/u");
            return kErrWire;
        }
        const auto range = p::UpdateRange::try_create(p::UpdateId{first_u}, p::UpdateId{final_u});
        if (!range.has_value()) {
            set_error("diff_depth range has U > u");
            return kErrWire;
        }
        if (out_first_u != nullptr) {
            *out_first_u = first_u;
        }
        if (out_final_u != nullptr) {
            *out_final_u = final_u;
        }

        std::optional<p::UpdateId> prev_final;
        if (!h->is_spot) {
            element pu_elem;
            if (obj["pu"].get(pu_elem) == simdjson::SUCCESS && !pu_elem.is_null()) {
                std::uint64_t pu_val = 0;
                if (pu_elem.get_uint64().get(pu_val) != simdjson::SUCCESS) {
                    set_error("diff_depth invalid pu uint64");
                    return kErrWire;
                }
                prev_final.emplace(pu_val);
            }
        }

        h->updates.clear();
        for (const auto side : {p::BookSide::Bid, p::BookSide::Ask}) {
            const char* field = side == p::BookSide::Bid ? "b" : "a";
            element side_elem;
            if (obj[field].get(side_elem) != simdjson::SUCCESS || side_elem.is_null()) {
                continue;
            }
            const int rc = parse_levels_array(
                side_elem, h->spec, field,
                [&](p::PriceUnits px, p::QuantityUnits qty) {
                    h->updates.push_back({side, px, qty});
                });
            if (rc != kOk) {
                return rc;
            }
        }

        const auto res = h->book.apply({*range, prev_final, h->updates});
        switch (res.disposition) {
        case p::ApplyDisposition::Applied: {
            const auto opt_book = h->book.synchronized_book();
            if (!opt_book.has_value()) {
                return kErrState;
            }
            const auto& bk = opt_book->get();
            const auto bid = bk.best_bid();
            const auto ask = bk.best_ask();
            int flags = kOk;
            if (!bid.has_value() || !ask.has_value()) {
                flags |= kEmptySideMask;
            } else if (bid->price >= ask->price) {
                flags |= kCrossedBookMask;
            }
            return flags;
        }
        case p::ApplyDisposition::IgnoredStale:
        case p::ApplyDisposition::IgnoredDuplicate:
            return kIgnored;
        case p::ApplyDisposition::GapDetected:
            return kGap;
        case p::ApplyDisposition::RejectedWrongState:
            set_error("apply called while BookProjection not ready");
            return kErrState;
        }
        return kErrState;
    } catch (const std::exception& ex) {
        set_error(ex.what());
        return kErrWire;
    }
}

int bmcp_book_top_levels(void* raw_handle, unsigned limit, std::uint64_t* out_uid,
                         std::int64_t* out_pairs, unsigned* out_counts) noexcept {
    const auto* h = static_cast<const BookHandle*>(raw_handle);
    if (h == nullptr || out_uid == nullptr || out_pairs == nullptr || out_counts == nullptr) {
        set_error("null argument to bmcp_book_top_levels");
        return kErrArg;
    }
    const auto opt_book = h->book.synchronized_book();
    const auto opt_uid = h->book.last_update_id();
    if (!opt_book.has_value() || !opt_uid.has_value()) {
        set_error("order book is not synchronized");
        return kErrState;
    }
    *out_uid = opt_uid->value();
    const auto& bk = opt_book->get();
    unsigned side_idx = 0;
    for (const auto side : {p::BookSide::Bid, p::BookSide::Ask}) {
        const auto levels = bk.top_levels(side, limit);
        out_counts[side_idx++] = static_cast<unsigned>(levels.size());
        for (const auto& lv : levels) {
            *out_pairs++ = lv.price.value();
            *out_pairs++ = lv.quantity.value();
        }
    }
    return kOk;
}

} // extern "C"
