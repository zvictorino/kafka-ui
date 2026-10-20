import styled from 'styled-components';
import { ControlPanelWrapper } from 'components/common/ControlPanel/ControlPanel.styled';

export const Container = styled.div``;

// The ACL page has a search box on the left and the "show system users" toggle pushed to the
// right. This page has no search, so keep the toggle on the right instead of letting
// space-between park it on the left.
export const ControlPanel = styled(ControlPanelWrapper)`
  justify-content: flex-end;
`;

export const DeleteCell = styled.div`
  display: flex;
`;
